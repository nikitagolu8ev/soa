package main

import (
	"crypto/rsa"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"os"
	"strconv"

	eh "error_handling"
	pb "proto"

	"github.com/IBM/sarama"
	"github.com/go-redis/redis/v8"
	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/mux"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

type UserData struct {
	Login       string `json:"login"`
	UserId      int    `json:"user_id"`
	Password    string `json:"password"`
	Name        string `json:"name"`
	Surname     string `json:"surname"`
	DateOfBirth string `json:"date_of_birth"`
	Email       string `json:"email"`
	PhoneNumber string `json:"phone_number"`
}

type MainServer struct {
	RedisDB          *redis.Client
	KafkaProducer    sarama.SyncProducer
	PostServerClient pb.PostManagerClient
	PrivateKey       *rsa.PrivateKey
	PublicKey        *rsa.PublicKey
}

type Event struct {
	PostId int    `json:"post_id"`
	UserId string `json:"user_id"`
}

func (server MainServer) GetUserId(login string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(login))
	return h.Sum64()
}

func (server MainServer) Register(writer http.ResponseWriter, request *http.Request) {
	var user_data UserData
	if eh.CheckHttp(json.NewDecoder(request.Body).Decode(&user_data), "Invalid user data", http.StatusBadRequest, writer) {
		return
	}
	exists, err := server.RedisDB.Exists(request.Context(), user_data.Login).Result()
	if eh.CheckHttp(err, "Redis database error", http.StatusInternalServerError, writer) ||
		eh.CheckConditionHttp(exists != 0, "User with this login already exists", http.StatusConflict, writer) {
		return
	}
	hashed_password, err := bcrypt.GenerateFromPassword([]byte(user_data.Password), bcrypt.DefaultCost)
	if eh.CheckHttp(err, fmt.Sprintf("Bad password `%s`", user_data.Password), http.StatusBadRequest, writer) {
		return
	}
	user_data.Password = string(hashed_password)
	json_user, _ := json.Marshal(user_data)
	if eh.CheckHttp(server.RedisDB.Set(request.Context(), user_data.Login, json_user, 0).Err(),
		"Redis database error", http.StatusInternalServerError, writer) {
		return
	}

	token, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"login": user_data.Login,
	}).SignedString(server.PrivateKey)
	if eh.CheckHttp(err, "Can't sign token", http.StatusInternalServerError, writer) {
		return
	}

	http.SetCookie(writer, &http.Cookie{
		Name:   "token",
		Value:  token,
		MaxAge: 24 * 60 * 60,
	})
	writer.WriteHeader(http.StatusOK)
}

func (server MainServer) Login(writer http.ResponseWriter, request *http.Request) {
	var user_data UserData
	if eh.CheckHttp(json.NewDecoder(request.Body).Decode(&user_data), "Invalid user data", http.StatusBadRequest, writer) {
		return
	}
	json_user, err := server.RedisDB.Get(request.Context(), user_data.Login).Result()
	if eh.CheckConditionHttp(err == redis.Nil, fmt.Sprintf("User with login `%v` doesn't exist", user_data.Login), http.StatusBadRequest, writer) ||
		eh.CheckHttp(err, "Redis database error", http.StatusInternalServerError, writer) {
		return
	}
	var saved_user_data UserData
	json.Unmarshal([]byte(json_user), &saved_user_data)
	if eh.CheckHttp(bcrypt.CompareHashAndPassword([]byte(saved_user_data.Password), []byte(user_data.Password)),
		"Invalid password", http.StatusBadRequest, writer) {
		return
	}
	hashed_password, err := bcrypt.GenerateFromPassword([]byte(user_data.Password), bcrypt.DefaultCost)
	if eh.CheckHttp(err, "Bad password", http.StatusBadRequest, writer) {
		return
	}

	user_data.Password = string(hashed_password)
	user_json, _ := json.Marshal(user_data)
	if eh.CheckHttp(server.RedisDB.Set(request.Context(), user_data.Login, user_json, 0).Err(), "Redis database error", http.StatusInternalServerError, writer) {
		return
	}

	token, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"login": user_data.Login,
	}).SignedString(server.PrivateKey)
	if eh.CheckHttp(err, "Can't sign token", http.StatusInternalServerError, writer) {
		return
	}
	http.SetCookie(writer, &http.Cookie{
		Name:   "token",
		Value:  token,
		MaxAge: 24 * 60 * 60,
	})
	writer.WriteHeader(http.StatusOK)
}

func (server MainServer) AuthorisedUser(request *http.Request) (string, error) {
	cookie, err := request.Cookie("token")
	if err != nil {
		return "", errors.New("no authorized user")
	}

	claims := jwt.MapClaims{}
	_, err = jwt.ParseWithClaims(cookie.Value, claims, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, errors.New("unexpected signing method")
		}
		return server.PublicKey, nil
	})
	if err != nil {
		return "", errors.New("no authorized user")
	}
	return claims["login"].(string), nil
}

func (server MainServer) UpdateUserData(writer http.ResponseWriter, request *http.Request) {
	var user_data UserData
	if eh.CheckHttp(json.NewDecoder(request.Body).Decode(&user_data), "Invalid user data", http.StatusBadRequest, writer) {
		return
	}
	login, err := server.AuthorisedUser(request)
	if eh.CheckHttp(err, "User is unauthorized", http.StatusUnauthorized, writer) {
		return
	}
	user_data.Login = login

	json_saved_user_data, err := server.RedisDB.Get(request.Context(), login).Result()
	if eh.CheckConditionHttp(err == redis.Nil, fmt.Sprintf("User with login `%v` doesn't exist", user_data.Login), http.StatusBadRequest, writer) ||
		eh.CheckHttp(err, "Redis database error", http.StatusInternalServerError, writer) {
		return
	}

	var saved_user_data UserData
	json.Unmarshal([]byte(json_saved_user_data), &saved_user_data)

	user_data.Login = saved_user_data.Login
	user_data.Password = saved_user_data.Password
	json_user, _ := json.Marshal(user_data)

	if eh.CheckHttp(server.RedisDB.Set(request.Context(), user_data.Login, json_user, 0).Err(),
		"Redis database error", http.StatusInternalServerError, writer) {
		return
	}

	writer.WriteHeader(http.StatusOK)
}

func (server MainServer) CreatePost(writer http.ResponseWriter, request *http.Request) {
	login, err := server.AuthorisedUser(request)
	if eh.CheckHttp(err, "User is unauthorized", http.StatusUnauthorized, writer) {
		return
	}

	body, err := io.ReadAll(request.Body)
	if eh.CheckHttp(err, "Unable to read request body", http.StatusBadRequest, writer) {
		return
	}

	grpc_request := pb.CreatePostRequest{}
	if eh.CheckHttp(protojson.Unmarshal(body, &grpc_request), "Invalud request", http.StatusBadRequest, writer) {
		return
	}
	grpc_request.Author = login

	grpc_response, err := server.PostServerClient.CreatePost(request.Context(), &grpc_request)
	status.FromError(err)
	if eh.CheckHttp(err, "grpc", http.StatusBadRequest, writer) {
		return
	}

	response, err := json.Marshal(grpc_response)
	if eh.CheckHttp(err, "json marshal", http.StatusInternalServerError, writer) {
		return
	}
	writer.Write(response)
}

func (server MainServer) UpdatePost(writer http.ResponseWriter, request *http.Request) {
	login, err := server.AuthorisedUser(request)
	if eh.CheckHttp(err, "User is unauthorized", http.StatusUnauthorized, writer) {
		return
	}

	vars := mux.Vars(request)
	post_id_str := vars["post_id"]
	post_id, err := strconv.ParseUint(post_id_str, 10, 32)
	if eh.CheckHttp(err, "Can't parse post id", http.StatusBadRequest, writer) {
		return
	}

	body, err := io.ReadAll(request.Body)
	if eh.CheckHttp(err, "Unable to read request body", http.StatusBadRequest, writer) {
		return
	}

	grpc_request := pb.UpdatePostRequest{}
	if err = protojson.Unmarshal(body, &grpc_request); err != nil {
		http.Error(writer, "Invalid request", http.StatusBadRequest)
		return
	}
	grpc_request.Author = login
	grpc_request.PostId = uint32(post_id)

	_, err = server.PostServerClient.UpdatePost(request.Context(), &grpc_request)
	status, _ := status.FromError(err)
	if eh.CheckGrpcHttp(status, "post service grpc error", writer) {
		return
	}

	writer.WriteHeader(http.StatusOK)
}

func (server MainServer) DeletePost(writer http.ResponseWriter, request *http.Request) {
	login, err := server.AuthorisedUser(request)
	if eh.CheckHttp(err, "User is unauthorized", http.StatusUnauthorized, writer) {
		return
	}

	vars := mux.Vars(request)
	post_id_str := vars["post_id"]
	post_id, err := strconv.ParseUint(post_id_str, 10, 32)
	if eh.CheckHttp(err, "Can't parse post id", http.StatusBadRequest, writer) {
		return
	}

	grpc_request := pb.DeletePostRequest{}
	grpc_request.Author = login
	grpc_request.PostId = uint32(post_id)

	_, err = server.PostServerClient.DeletePost(request.Context(), &grpc_request)
	status, _ := status.FromError(err)
	if eh.CheckGrpcHttp(status, "post service grpc error", writer) {
		return
	}
	writer.WriteHeader(http.StatusOK)
}

func (server MainServer) GetPostById(writer http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	post_id_str := vars["post_id"]
	post_id, err := strconv.ParseUint(post_id_str, 10, 32)
	if eh.CheckHttp(err, "Can't parse post id", http.StatusBadRequest, writer) {
		return
	}

	grpc_request := pb.GetPostByIdRequest{}
	grpc_request.PostId = uint32(post_id)

	grpc_response, err := server.PostServerClient.GetPostById(request.Context(), &grpc_request)
	status, _ := status.FromError(err)
	if eh.CheckGrpcHttp(status, "post service grpc error", writer) {
		return
	}

	response, err := json.Marshal(grpc_response)
	if eh.CheckHttp(err, "Json marshal error", http.StatusInternalServerError, writer) {
		return
	}
	writer.Write(response)
}

func (server MainServer) GetPostsOnPage(writer http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	page_id_str := vars["page_id"]
	page_id, err := strconv.ParseUint(page_id_str, 10, 32)
	if eh.CheckHttp(err, "Can't parse page id", http.StatusBadRequest, writer) {
		return
	}

	grpc_request := pb.GetPostsOnPageRequest{}
	grpc_request.PageId = uint32(page_id)

	grpc_response, err := server.PostServerClient.GetPostsOnPage(request.Context(), &grpc_request)
	status, _ := status.FromError(err)
	if eh.CheckGrpcHttp(status, "post service grpc error", writer) {
		return
	}

	if eh.CheckConditionHttp(len(grpc_response.Posts) == 0, fmt.Sprintf("No posts found on page `%d`", page_id), http.StatusBadRequest, writer) {
		return
	}

	response, err := json.Marshal(grpc_response)
	if eh.CheckHttp(err, "Json marshal error", http.StatusInternalServerError, writer) {
		return
	}
	writer.Write(response)
}

func (server MainServer) LikePost(writer http.ResponseWriter, request *http.Request) {
	login, err := server.AuthorisedUser(request)
	if eh.CheckHttp(err, "User is unauthorized", http.StatusUnauthorized, writer) {
		return
	}

	vars := mux.Vars(request)
	post_id_str := vars["post_id"]
	post_id, err := strconv.ParseUint(post_id_str, 10, 32)
	if eh.CheckHttp(err, "Can't parse post id", http.StatusBadRequest, writer) {
		return
	}

	var like Event
	like.PostId = int(post_id)
	like.UserId = login

	message_payload, err := json.Marshal(like)
	if eh.CheckHttp(err, "Json marshal error", http.StatusInternalServerError, writer) {
		return
	}

	message := &sarama.ProducerMessage{Topic: "like_topic", Value: sarama.ByteEncoder(message_payload)}

	fmt.Fprintf(os.Stderr, "Start sending into kafka: %s\n", message_payload)
	if _, _, err = server.KafkaProducer.SendMessage(message); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to send message to kafka: %v\n", err)
		http.Error(writer, fmt.Sprintf("Failed to send like event to broker: %v", err), http.StatusInternalServerError)
		return
	}

	writer.WriteHeader(http.StatusOK)
}

func (server MainServer) ViewPost(writer http.ResponseWriter, request *http.Request) {
	login, err := server.AuthorisedUser(request)
	if eh.CheckHttp(err, "User is unauthorized", http.StatusUnauthorized, writer) {
		return
	}

	vars := mux.Vars(request)
	post_id_str := vars["post_id"]
	post_id, err := strconv.ParseUint(post_id_str, 10, 32)
	if eh.CheckHttp(err, "Can't parse post id", http.StatusBadRequest, writer) {
		return
	}

	var view Event
	view.PostId = int(post_id)
	view.UserId = login

	message_payload, err := json.Marshal(view)
	if eh.CheckHttp(err, "Json marshal error", http.StatusInternalServerError, writer) {
		return
	}

	message := &sarama.ProducerMessage{Topic: "view_topic", Value: sarama.ByteEncoder(message_payload)}

	fmt.Fprintf(os.Stderr, "Start sending into kafka: %s\n", message_payload)
	if _, _, err = server.KafkaProducer.SendMessage(message); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to send message to kafka: %v\n", err)
		http.Error(writer, fmt.Sprintf("Failed to send like event to broker: %v", err), http.StatusInternalServerError)
		return
	}

	writer.WriteHeader(http.StatusOK)
}

func main() {
	private_key_path := flag.String("private_key_path", "", "path to private key file")
	public_key_path := flag.String("public_key_path", "", "path to public key file")
	server_port := flag.Int("port", 4200, "server port")
	post_server_port := flag.Int("post_server_port", 50051, "post server port")
	redis_port := flag.Int("redis_port", 6379, "redis database port")
	flag.Parse()

	eh.CheckConditionCritical(*private_key_path == "", "No path to private key file")
	eh.CheckConditionCritical(*public_key_path == "", "No path to public key file")

	var server MainServer
	var err error
	server.KafkaProducer, err = sarama.NewSyncProducer([]string{"kafka:9092"}, nil)
	eh.CheckCritical(err, "Failed to connect to kafka")

	conn, err := grpc.NewClient(fmt.Sprintf("post_service:%d", *post_server_port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	eh.CheckCritical(err, "Failed to connect to post server")
	defer conn.Close()
	server.PostServerClient = pb.NewPostManagerClient(conn)

	private_key, err := os.ReadFile(*private_key_path)
	eh.CheckCritical(err, "private key")
	public_key, err := os.ReadFile(*public_key_path)
	eh.CheckCritical(err, "public key")

	server.PrivateKey, err = jwt.ParseRSAPrivateKeyFromPEM(private_key)
	eh.CheckCritical(err, "jwt private key")
	server.PublicKey, err = jwt.ParseRSAPublicKeyFromPEM(public_key)
	eh.CheckCritical(err, "jwt public key")

	server.RedisDB = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("redis:%d", *redis_port),
		Password: "",
		DB:       0,
	})

	router := mux.NewRouter()
	router.HandleFunc("/users/register", server.Register).Methods("POST")
	router.HandleFunc("/users/login", server.Login).Methods("POST")
	router.HandleFunc("/users/", server.UpdateUserData).Methods("PUT")
	router.HandleFunc("/posts/", server.CreatePost).Methods("POST")
	router.HandleFunc("/posts/{post_id}", server.UpdatePost).Methods("PUT")
	router.HandleFunc("/posts/{post_id}", server.DeletePost).Methods("DELETE")
	router.HandleFunc("/posts/{post_id}", server.GetPostById).Methods("GET")
	router.HandleFunc("/posts/page/{page_id}", server.GetPostsOnPage).Methods("GET")
	router.HandleFunc("/posts/view/{post_id}", server.ViewPost).Methods("PUT")
	router.HandleFunc("/posts/like/{post_id}", server.LikePost).Methods("PUT")

	fmt.Printf("Starting serving on port: %d\n", *server_port)

	eh.CheckCritical(http.ListenAndServe(fmt.Sprintf(":%d", *server_port), router), "main_service")
}
