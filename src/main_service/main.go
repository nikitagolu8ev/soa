package main

import (
	"context"
	"crypto/rsa"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"

	eh "error_handling"
	pb "proto"

	_ "github.com/lib/pq"

	"github.com/IBM/sarama"
	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/mux"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
)

type UserAuth struct {
	Login    string `json:"login"`
	Password string `json:"password"`
}

type UserData struct {
	Name        string `json:"name"`
	Surname     string `json:"surname"`
	DateOfBirth string `json:"date_of_birth"`
	Email       string `json:"email"`
	PhoneNumber string `json:"phone_number"`
}

type MainServer struct {
	DataBase         *sql.DB
	KafkaProducer    sarama.SyncProducer
	PostServerClient pb.PostManagerClient
	StatServerClient pb.StatManagerClient
	PrivateKey       *rsa.PrivateKey
	PublicKey        *rsa.PublicKey
}

type Event struct {
	PostId   int64 `json:"post_id"`
	AuthorId int64 `json:"author_id"`
	UserId   int64 `json:"user_id"`
}

func (server MainServer) GetUserId(ctx context.Context, login string) *int64 {
	var user_id int64
	if server.DataBase.QueryRowContext(ctx, `SELECT user_id FROM user_data WHERE login = $1`, login).Scan(&user_id) != nil {
		return nil
	}
	return &user_id
}

func (server MainServer) GetUserLogin(ctx context.Context, user_id int64) (string, error) {
	var login string
	if err := server.DataBase.QueryRowContext(ctx, `SELECT login FROM user_data WHERE user_id = $1`, user_id).Scan(&login); err != nil {
		return "", err
	}
	return login, nil

}

func (server MainServer) Register(writer http.ResponseWriter, request *http.Request) {
	var user_auth UserAuth
	if eh.CheckHttp(json.NewDecoder(request.Body).Decode(&user_auth), "Invalid user data", http.StatusBadRequest, writer) {
		return
	}
	if eh.CheckConditionHttp(server.GetUserId(request.Context(), user_auth.Login) != nil, "User with this login already exists", http.StatusConflict, writer) {
		return
	}
	hashed_password, err := bcrypt.GenerateFromPassword([]byte(user_auth.Password), bcrypt.DefaultCost)
	if eh.CheckHttp(err, fmt.Sprintf("Bad password `%s`", user_auth.Password), http.StatusBadRequest, writer) {
		return
	}
	var user_id int64
	if eh.CheckHttp(server.DataBase.QueryRowContext(request.Context(), `INSERT INTO user_data (login, hashed_password) VALUES ($1, $2) RETURNING user_id`,
		user_auth.Login, string(hashed_password)).Scan(&user_id), "Can't update user database", http.StatusInternalServerError, writer) {
		return
	}

	token, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"user_id": user_id,
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
	var user_data UserAuth
	if eh.CheckHttp(json.NewDecoder(request.Body).Decode(&user_data), "Invalid user data", http.StatusBadRequest, writer) {
		return
	}
	var user_id int64
	var hashed_password string
	err := server.DataBase.QueryRowContext(request.Context(), `SELECT user_id, hashed_password FROM user_data WHERE login = $1`,
		user_data.Login).Scan(&user_id, &hashed_password)
	if eh.CheckConditionHttp(err == sql.ErrNoRows, "No registered user with such login", http.StatusBadRequest, writer) ||
		eh.CheckHttp(err, "Can't reach user database", http.StatusInternalServerError, writer) {
		return
	}
	if eh.CheckHttp(bcrypt.CompareHashAndPassword([]byte(hashed_password), []byte(user_data.Password)),
		"Invalid password", http.StatusBadRequest, writer) {
		return
	}

	token, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"user_id": user_id,
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

func (server MainServer) AuthorisedUser(request *http.Request) (int64, error) {
	cookie, err := request.Cookie("token")
	if err != nil {
		return 0, errors.New("no authorized user")
	}

	claims := jwt.MapClaims{}
	_, err = jwt.ParseWithClaims(cookie.Value, claims, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, errors.New("unexpected signing method")
		}
		return server.PublicKey, nil
	})
	if err != nil {
		return 0, errors.New("no authorized user")
	}
	return int64(claims["user_id"].(float64)), nil
}

func (server MainServer) UpdateUserData(writer http.ResponseWriter, request *http.Request) {
	var user_data UserData
	if eh.CheckHttp(json.NewDecoder(request.Body).Decode(&user_data), "Invalid user data", http.StatusBadRequest, writer) {
		return
	}
	user_id, err := server.AuthorisedUser(request)
	if eh.CheckHttp(err, "User is unauthorized", http.StatusUnauthorized, writer) {
		return
	}

	json_user, _ := json.Marshal(user_data)

	_, err = server.DataBase.ExecContext(request.Context(), `UPDATE user_data SET data = $1 where user_id = $2`, json_user, user_id)
	if eh.CheckHttp(err, "main database error", http.StatusInternalServerError, writer) {
		return
	}

	writer.WriteHeader(http.StatusOK)
}

func (server MainServer) CreatePost(writer http.ResponseWriter, request *http.Request) {
	user_id, err := server.AuthorisedUser(request)
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
	grpc_request.AuthorId = user_id

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
	user_id, err := server.AuthorisedUser(request)
	if eh.CheckHttp(err, "User is unauthorized", http.StatusUnauthorized, writer) {
		return
	}

	vars := mux.Vars(request)
	post_id_str := vars["post_id"]
	post_id, err := strconv.ParseInt(post_id_str, 10, 64)
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
	grpc_request.AuthorId = user_id
	grpc_request.PostId = post_id

	_, err = server.PostServerClient.UpdatePost(request.Context(), &grpc_request)
	status, _ := status.FromError(err)
	if eh.CheckGrpcHttp(status, "post service grpc error", writer) {
		return
	}

	writer.WriteHeader(http.StatusOK)
}

func (server MainServer) DeletePost(writer http.ResponseWriter, request *http.Request) {
	user_id, err := server.AuthorisedUser(request)
	if eh.CheckHttp(err, "User is unauthorized", http.StatusUnauthorized, writer) {
		return
	}

	vars := mux.Vars(request)
	post_id_str := vars["post_id"]
	post_id, err := strconv.ParseInt(post_id_str, 10, 64)
	if eh.CheckHttp(err, "Can't parse post id", http.StatusBadRequest, writer) {
		return
	}

	grpc_request := pb.DeletePostRequest{}
	grpc_request.AuthorId = user_id
	grpc_request.PostId = post_id

	_, err = server.PostServerClient.DeletePost(request.Context(), &grpc_request)
	s, _ := status.FromError(err)
	if eh.CheckGrpcHttp(s, "post service grpc error", writer) {
		return
	}

	_, err = server.StatServerClient.DeletePost(request.Context(), &grpc_request)
	s, _ = status.FromError(err)
	if eh.CheckGrpcHttp(s, "stat service grpc error", writer) {
		return
	}
	writer.WriteHeader(http.StatusOK)
}

func (server MainServer) GetPostById(writer http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	post_id_str := vars["post_id"]
	post_id, err := strconv.ParseInt(post_id_str, 10, 64)
	if eh.CheckHttp(err, "Can't parse post id", http.StatusBadRequest, writer) {
		return
	}

	grpc_request := pb.GetPostByIdRequest{}
	grpc_request.PostId = post_id

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
	page_id, err := strconv.ParseInt(page_id_str, 10, 64)
	if eh.CheckHttp(err, "Can't parse page id", http.StatusBadRequest, writer) {
		return
	}

	grpc_request := pb.GetPostsOnPageRequest{}
	grpc_request.PageId = page_id

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
	user_id, err := server.AuthorisedUser(request)
	if eh.CheckHttp(err, "User is unauthorized", http.StatusUnauthorized, writer) {
		return
	}

	vars := mux.Vars(request)
	post_id_str := vars["post_id"]
	post_id, err := strconv.ParseInt(post_id_str, 10, 64)
	if eh.CheckHttp(err, "Can't parse post id", http.StatusBadRequest, writer) {
		return
	}

	grpc_request := pb.GetPostByIdRequest{}
	grpc_request.PostId = post_id

	grpc_response, err := server.PostServerClient.GetPostById(request.Context(), &grpc_request)
	status, _ := status.FromError(err)
	if eh.CheckGrpcHttp(status, "post service grpc error", writer) {
		return
	}

	author_id := grpc_response.Post.AuthorId

	var like Event
	like.PostId = int64(post_id)
	like.UserId = user_id
	like.AuthorId = author_id

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
	user_id, err := server.AuthorisedUser(request)
	if eh.CheckHttp(err, "User is unauthorized", http.StatusUnauthorized, writer) {
		return
	}

	vars := mux.Vars(request)
	post_id_str := vars["post_id"]
	post_id, err := strconv.ParseInt(post_id_str, 10, 64)
	if eh.CheckHttp(err, "Can't parse post id", http.StatusBadRequest, writer) {
		return
	}

	grpc_request := pb.GetPostByIdRequest{}
	grpc_request.PostId = post_id

	grpc_response, err := server.PostServerClient.GetPostById(request.Context(), &grpc_request)
	status, _ := status.FromError(err)
	if eh.CheckGrpcHttp(status, "post service grpc error", writer) {
		return
	}

	author_id := grpc_response.Post.AuthorId

	var view Event
	view.PostId = post_id
	view.AuthorId = author_id
	view.UserId = user_id

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

func (server MainServer) GetPostStats(writer http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	post_id_str := vars["post_id"]
	post_id, err := strconv.ParseInt(post_id_str, 10, 64)
	if eh.CheckHttp(err, "Can't parse post id", http.StatusBadRequest, writer) {
		return
	}

	grpc_request := pb.GetPostStatsRequest{}
	grpc_request.PostId = post_id

	grpc_response, err := server.StatServerClient.GetPostStats(request.Context(), &grpc_request)
	status, _ := status.FromError(err)
	if eh.CheckGrpcHttp(status, "stat service grpc error", writer) {
		return
	}

	response, err := json.Marshal(grpc_response)
	if eh.CheckHttp(err, "Json marshal error", http.StatusInternalServerError, writer) {
		return
	}
	writer.Write(response)
}

func (server MainServer) GetTopPosts(writer http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	order_by := vars["order_by"]

	grpc_request := pb.GetTopPostsRequest{}
	if order_by == "likes" {
		grpc_request.OrderBy = pb.OrderPostsBy_LIKES
	} else if order_by == "views" {
		grpc_request.OrderBy = pb.OrderPostsBy_VIEWS
	} else {
		http.Error(writer, "Invalid order_by argument", http.StatusBadRequest)
		return
	}

	grpc_response, err := server.StatServerClient.GetTopPosts(request.Context(), &grpc_request)
	status, _ := status.FromError(err)
	if eh.CheckGrpcHttp(status, "stat service grpc error", writer) {
		return
	}

	var result pb.TopPosts
	for _, stats := range grpc_response.PostStats {
		var final_stats pb.FinalPostStats
		final_stats.PostId = stats.PostId
		final_stats.Stat = stats.Stat
		final_stats.Author, err = server.GetUserLogin(request.Context(), stats.AuthorId)
		if eh.CheckHttp(err, "Can't find login by user id", http.StatusInternalServerError, writer) {
			return
		}
		result.PostStats = append(result.PostStats, &final_stats)
	}

	response, err := json.Marshal(&result)
	if eh.CheckHttp(err, "Json marshal error", http.StatusInternalServerError, writer) {
		return
	}
	writer.Write(response)
}

func (server MainServer) GetTopAuthors(writer http.ResponseWriter, request *http.Request) {
	grpc_response, err := server.StatServerClient.GetTopAuthors(request.Context(), &emptypb.Empty{})
	status, _ := status.FromError(err)
	if eh.CheckGrpcHttp(status, "stat service grpc error", writer) {
		return
	}

	var result pb.TopAuthors
	for _, stats := range grpc_response.AuthorStats {
		var final_stats pb.FinalAuthorStats
		final_stats.Likes = stats.Likes
		final_stats.Author, err = server.GetUserLogin(request.Context(), stats.AuthorId)
		if eh.CheckHttp(err, "Can't find login by user id", http.StatusInternalServerError, writer) {
			return
		}
		result.AuthorStats = append(result.AuthorStats, &final_stats)
	}

	response, err := json.Marshal(&result)
	if eh.CheckHttp(err, "Json marshal error", http.StatusInternalServerError, writer) {
		return
	}
	writer.Write(response)

}

func main() {
	private_key_path := flag.String("private_key_path", "", "path to private key file")
	public_key_path := flag.String("public_key_path", "", "path to public key file")
	server_port := flag.Int("port", 4200, "server port")
	post_server_port := flag.Int("post_server_port", 50051, "post server port")
	stat_server_port := flag.Int("stat_server_port", 8192, "stat server port")
	flag.Parse()

	eh.CheckConditionCritical(*private_key_path == "", "No path to private key file")
	eh.CheckConditionCritical(*public_key_path == "", "No path to public key file")

	var server MainServer
	var err error
	server.KafkaProducer, err = sarama.NewSyncProducer([]string{"kafka:9092"}, nil)
	eh.CheckCritical(err, "Failed to connect to kafka")

	post_conn, err := grpc.NewClient(fmt.Sprintf("post_service:%d", *post_server_port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	eh.CheckCritical(err, "Failed to connect to post server")
	defer post_conn.Close()
	server.PostServerClient = pb.NewPostManagerClient(post_conn)

	stat_conn, err := grpc.NewClient(fmt.Sprintf("stat_service:%d", *stat_server_port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	eh.CheckCritical(err, "Failed to connect to stat server")
	defer stat_conn.Close()
	server.StatServerClient = pb.NewStatManagerClient(stat_conn)

	private_key, err := os.ReadFile(*private_key_path)
	eh.CheckCritical(err, "private key")
	public_key, err := os.ReadFile(*public_key_path)
	eh.CheckCritical(err, "public key")

	server.PrivateKey, err = jwt.ParseRSAPrivateKeyFromPEM(private_key)
	eh.CheckCritical(err, "jwt private key")
	server.PublicKey, err = jwt.ParseRSAPublicKeyFromPEM(public_key)
	eh.CheckCritical(err, "jwt public key")

	server.DataBase, err = sql.Open("postgres", "host=userdata_postgres port=5432 user=main_service password=password dbname=userdata_db sslmode=disable")
	eh.CheckCritical(err, "Failed to open main service database")
	eh.CheckCritical(server.DataBase.Ping(), "Failed to reach main service database")

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
	router.HandleFunc("/posts/stats/{post_id}", server.GetPostStats).Methods("GET")
	router.HandleFunc("/posts/top/{order_by}", server.GetTopPosts).Methods("GET")
	router.HandleFunc("/users/top", server.GetTopAuthors).Methods("GET")

	fmt.Printf("Starting serving on port: %d\n", *server_port)

	eh.CheckCritical(http.ListenAndServe(fmt.Sprintf(":%d", *server_port), router), "main_service")
}
