package main

import (
	"crypto/rsa"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	pb "proto"

	"github.com/IBM/sarama"
	"github.com/go-redis/redis/v8"
	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/mux"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
)

type UserData struct {
	Login       string `json:"login"`
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

func (server MainServer) Register(writer http.ResponseWriter, request *http.Request) {
	var user_data UserData
	if err := json.NewDecoder(request.Body).Decode(&user_data); err != nil {
		http.Error(writer, "Invalid user data", http.StatusBadRequest)
		return
	}
	exists, err := server.RedisDB.Exists(request.Context(), user_data.Login).Result()
	if err != nil {
		http.Error(writer, fmt.Sprintf("Redis darabase error: %v", err), http.StatusInternalServerError)
		return
	}
	if exists != 0 {
		http.Error(writer, "User with this login already exists", http.StatusConflict)
		return
	}
	hashed_password, err := bcrypt.GenerateFromPassword([]byte(user_data.Password), bcrypt.DefaultCost)
	if err != nil {
		http.Error(writer, fmt.Sprintf("Bad password `%v`: %v", user_data.Password, err),
			http.StatusBadRequest)
	}
	user_data.Password = string(hashed_password)
	json_user, _ := json.Marshal(user_data)
	if err = server.RedisDB.Set(request.Context(), user_data.Login, json_user, 0).Err(); err != nil {
		http.Error(writer, fmt.Sprintf("Redis darabase error: %v", err), http.StatusInternalServerError)
		return
	}

	token, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"login": user_data.Login,
	}).SignedString(server.PrivateKey)
	if err != nil {
		http.Error(writer, fmt.Sprintf("Can't sign token : %v", err), http.StatusInternalServerError)
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
	if err := json.NewDecoder(request.Body).Decode(&user_data); err != nil {
		http.Error(writer, "Invalid user data", http.StatusBadRequest)
		return
	}
	json_user, err := server.RedisDB.Get(request.Context(), user_data.Login).Result()
	if err != nil {
		if err == redis.Nil {
			http.Error(writer, fmt.Sprintf("User with login `%v` doesn't exist", user_data.Login),
				http.StatusBadRequest)
			return

		}
		http.Error(writer, fmt.Sprintf("Redis darabase error: %v", err), http.StatusInternalServerError)
		return
	}
	var saved_user_data UserData
	json.Unmarshal([]byte(json_user), &saved_user_data)
	if bcrypt.CompareHashAndPassword([]byte(saved_user_data.Password), []byte(user_data.Password)) != nil {
		http.Error(writer, "Invalid password", http.StatusBadRequest)
		return
	}
	hashed_password, err := bcrypt.GenerateFromPassword([]byte(user_data.Password), bcrypt.DefaultCost)
	if err != nil {
		http.Error(writer, fmt.Sprintf("Bad password `%v` password: %v", user_data.Password, err),
			http.StatusBadRequest)
	}
	user_data.Password = string(hashed_password)
	user_json, _ := json.Marshal(user_data)
	if err = server.RedisDB.Set(request.Context(), user_data.Login, user_json, 0).Err(); err != nil {
		http.Error(writer, fmt.Sprintf("Redis darabase error: %v", err), http.StatusInternalServerError)
		return
	}

	token, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"login": user_data.Login,
	}).SignedString(server.PrivateKey)
	if err != nil {
		http.Error(writer, fmt.Sprintf("Can't sign token : %v", err), http.StatusInternalServerError)
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
	if err := json.NewDecoder(request.Body).Decode(&user_data); err != nil {
		http.Error(writer, "Invalid user data", http.StatusBadRequest)
		return
	}
	login, err := server.AuthorisedUser(request)
	if err != nil {
		http.Error(writer, "User is unauthorized", http.StatusUnauthorized)
		return
	}

	json_saved_user_data, err := server.RedisDB.Get(request.Context(), login).Result()
	if err != nil {
		if err == redis.Nil {
			http.Error(writer, fmt.Sprintf("User with login `%v` doesn't exist", user_data.Login),
				http.StatusBadRequest)
			return

		}
		http.Error(writer, fmt.Sprintf("Redis darabase error: %v", err), http.StatusInternalServerError)
		return
	}

	var saved_user_data UserData
	json.Unmarshal([]byte(json_saved_user_data), &saved_user_data)

	user_data.Login = saved_user_data.Login
	user_data.Password = saved_user_data.Password
	json_user, _ := json.Marshal(user_data)

	if err = server.RedisDB.Set(request.Context(), user_data.Login, json_user, 0).Err(); err != nil {
		http.Error(writer, fmt.Sprintf("Redis darabase error: %v", err), http.StatusInternalServerError)
		return
	}

	writer.WriteHeader(http.StatusOK)
}

func (server MainServer) CreatePost(writer http.ResponseWriter, request *http.Request) {
	login, err := server.AuthorisedUser(request)
	if err != nil {
		http.Error(writer, "User is unauthorized", http.StatusUnauthorized)
		return
	}

	body, err := io.ReadAll(request.Body)
	if err != nil {
		http.Error(writer, "Unable to read request body", http.StatusBadRequest)
		return
	}

	grpc_request := pb.CreatePostRequest{}
	if err = protojson.Unmarshal(body, &grpc_request); err != nil {
		http.Error(writer, "Invalid request", http.StatusBadRequest)
		return
	}
	grpc_request.Author = login

	grpc_response, err := server.PostServerClient.CreatePost(request.Context(), &grpc_request)
	if err != nil {
		http.Error(writer, fmt.Sprintf("%v", err), http.StatusInternalServerError)
		return
	}

	response, err := json.Marshal(grpc_response)
	if err != nil {
		http.Error(writer, fmt.Sprintf("Json marshal error: %v", err), http.StatusInternalServerError)
		return
	}
	writer.Write(response)
}

func (server MainServer) UpdatePost(writer http.ResponseWriter, request *http.Request) {
	login, err := server.AuthorisedUser(request)
	if err != nil {
		http.Error(writer, "User is unauthorized", http.StatusUnauthorized)
		return
	}

	vars := mux.Vars(request)
	post_id_str := vars["post_id"]
	post_id, err := strconv.ParseUint(post_id_str, 10, 64)
	if err != nil {
		http.Error(writer, fmt.Sprintf("Can't parse post id: %v", err), http.StatusBadRequest)
		return
	}

	body, err := io.ReadAll(request.Body)
	if err != nil {
		http.Error(writer, "Unable to read request body", http.StatusBadRequest)
		return
	}

	grpc_request := pb.UpdatePostRequest{}
	if err = protojson.Unmarshal(body, &grpc_request); err != nil {
		http.Error(writer, "Invalid request", http.StatusBadRequest)
		return
	}
	grpc_request.Author = login
	grpc_request.PostId = post_id

	grpc_response, err := server.PostServerClient.UpdatePost(request.Context(), &grpc_request)
	if err != nil {
		http.Error(writer, fmt.Sprintf("%v", err), http.StatusInternalServerError)
		return
	}
	if !grpc_response.Successful {
		http.Error(writer, fmt.Sprintf("No post found with id : %d created by %s", post_id, login), http.StatusBadRequest)
		return
	}
	writer.WriteHeader(http.StatusOK)
}

func (server MainServer) DeletePost(writer http.ResponseWriter, request *http.Request) {
	login, err := server.AuthorisedUser(request)
	if err != nil {
		http.Error(writer, "User is unauthorized", http.StatusUnauthorized)
		return
	}

	vars := mux.Vars(request)
	post_id_str := vars["post_id"]
	post_id, err := strconv.ParseUint(post_id_str, 10, 64)
	if err != nil {
		http.Error(writer, fmt.Sprintf("Can't parse post id: %v", err), http.StatusBadRequest)
		return
	}

	grpc_request := pb.DeletePostRequest{}
	grpc_request.Author = login
	grpc_request.PostId = post_id

	grpc_response, err := server.PostServerClient.DeletePost(request.Context(), &grpc_request)
	if err != nil {
		http.Error(writer, fmt.Sprintf("%v", err), http.StatusInternalServerError)
		return
	}
	if !grpc_response.Successful {
		http.Error(writer, fmt.Sprintf("No post found with id : %d created by %s", post_id, login), http.StatusBadRequest)
		return
	}
	writer.WriteHeader(http.StatusOK)
}

func (server MainServer) GetPostById(writer http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	post_id_str := vars["post_id"]
	post_id, err := strconv.ParseUint(post_id_str, 10, 64)
	if err != nil {
		http.Error(writer, fmt.Sprintf("Can't parse post id: %v", err), http.StatusBadRequest)
		return
	}

	grpc_request := pb.GetPostByIdRequest{}
	grpc_request.PostId = post_id

	grpc_response, err := server.PostServerClient.GetPostById(request.Context(), &grpc_request)
	if err != nil {
		http.Error(writer, fmt.Sprintf("%v", err), http.StatusInternalServerError)
		return
	}

	response, err := json.Marshal(grpc_response)
	if err != nil {
		http.Error(writer, fmt.Sprintf("Json marshal error: %v", err), http.StatusInternalServerError)
		return
	}
	writer.Write(response)
}

func (server MainServer) GetPostsOnPage(writer http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	page_id_str := vars["page_id"]
	page_id, err := strconv.ParseUint(page_id_str, 10, 64)
	if err != nil {
		http.Error(writer, fmt.Sprintf("Can't parse page id: %v", err), http.StatusBadRequest)
		return
	}

	grpc_request := pb.GetPostsOnPageRequest{}
	grpc_request.PageId = page_id

	grpc_response, err := server.PostServerClient.GetPostsOnPage(request.Context(), &grpc_request)
	if err != nil {
		http.Error(writer, fmt.Sprintf("%v", err), http.StatusInternalServerError)
		return
	}

	if len(grpc_response.Posts) == 0 {
		http.Error(writer, fmt.Sprintf("No posts found on page %d", page_id), http.StatusBadRequest)
		return
	}

	response, err := json.Marshal(grpc_response)
	if err != nil {
		http.Error(writer, fmt.Sprintf("Json marshal error: %v", err), http.StatusInternalServerError)
		return
	}
	writer.Write(response)
}

func (server MainServer) LikePost(writer http.ResponseWriter, request *http.Request) {
	login, err := server.AuthorisedUser(request)
	if err != nil {
		http.Error(writer, "User is unauthorized", http.StatusUnauthorized)
		return
	}

	vars := mux.Vars(request)
	post_id_str := vars["post_id"]
	post_id, err := strconv.ParseUint(post_id_str, 10, 64)
	if err != nil {
		http.Error(writer, fmt.Sprintf("Can't parse post id: %v", err), http.StatusBadRequest)
		return
	}

	// timestamp := time.Now()

	message_payload := fmt.Sprint(post_id) + "," + login
	message := &sarama.ProducerMessage{Topic: "like_topic", Value: sarama.ByteEncoder(message_payload)}

	fmt.Fprintf(os.Stderr, "Start sending into kafka: %s", message_payload)
	if _, _, err = server.KafkaProducer.SendMessage(message); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to send message to kafka: %v\n", err)
		http.Error(writer, fmt.Sprintf("Failed to send like event to broker: %v", err), http.StatusInternalServerError)
		return
	}

	writer.WriteHeader(http.StatusOK)
}

func (server MainServer) ViewPost(writer http.ResponseWriter, request *http.Request) {
	login, err := server.AuthorisedUser(request)
	if err != nil {
		http.Error(writer, "User is unauthorized", http.StatusUnauthorized)
		return
	}

	vars := mux.Vars(request)
	post_id_str := vars["post_id"]
	post_id, err := strconv.ParseUint(post_id_str, 10, 64)
	if err != nil {
		http.Error(writer, fmt.Sprintf("Can't parse post id: %v", err), http.StatusBadRequest)
		return
	}

	timestamp := time.Now()

	message_payload := fmt.Sprint(post_id) + "," + login + "," + timestamp.String()
	message := &sarama.ProducerMessage{Topic: "view_topic", Value: sarama.ByteEncoder(message_payload)}

	fmt.Fprintf(os.Stderr, "Start sending into kafka: %s", message_payload)
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

	if *private_key_path == "" {
		fmt.Fprintln(os.Stderr, "No path to private key file")
		os.Exit(1)
	}

	if *public_key_path == "" {
		fmt.Fprintln(os.Stderr, "No path to public key file")
		os.Exit(1)
	}

	var server MainServer
	var err error
	server.KafkaProducer, err = sarama.NewSyncProducer([]string{"kafka:9092"}, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to kafka broker: %v", err)
		os.Exit(1)
	}

	conn, err := grpc.NewClient(fmt.Sprintf("post_service:%d", *post_server_port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to post server: %v", err)
		os.Exit(1)
	}
	defer conn.Close()
	server.PostServerClient = pb.NewPostManagerClient(conn)

	private_key, err := os.ReadFile(*private_key_path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	public_key, err := os.ReadFile(*public_key_path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if server.PrivateKey, err = jwt.ParseRSAPrivateKeyFromPEM(private_key); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if server.PublicKey, err = jwt.ParseRSAPublicKeyFromPEM(public_key); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	server.RedisDB = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("redis:%d", *redis_port),
		Password: "",
		DB:       0,
	})

	router := mux.NewRouter()
	router.HandleFunc("/users/register", server.Register).Methods("POST")
	router.HandleFunc("/users/login", server.Login).Methods("POST")
	router.HandleFunc("/users/update", server.UpdateUserData).Methods("PUT")
	router.HandleFunc("/posts/create", server.CreatePost).Methods("POST")
	router.HandleFunc("/posts/update/{post_id}", server.UpdatePost).Methods("PUT")
	router.HandleFunc("/posts/delete/{post_id}", server.DeletePost).Methods("DELETE")
	router.HandleFunc("/posts/get/{post_id}", server.GetPostById).Methods("GET")
	router.HandleFunc("/posts/page/{page_id}", server.GetPostsOnPage).Methods("GET")
	router.HandleFunc("/posts/view/{post_id}", server.ViewPost).Methods("PUT")
	router.HandleFunc("/posts/like/{post_id}", server.LikePost).Methods("PUT")

	fmt.Printf("Starting serving on port: %d\n", *server_port)

	if err = http.ListenAndServe(fmt.Sprintf(":%d", *server_port), router); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
