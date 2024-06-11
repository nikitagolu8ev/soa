package main

import (
	"crypto/rsa"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/go-redis/redis/v8"
	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/mux"
	"golang.org/x/crypto/bcrypt"
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

type Authenticator struct {
	RedisDB    *redis.Client
	PrivateKey *rsa.PrivateKey
	PublicKey  *rsa.PublicKey
}

func (auth Authenticator) Register(writer http.ResponseWriter, request *http.Request) {
	var user_data UserData
	if err := json.NewDecoder(request.Body).Decode(&user_data); err != nil {
		http.Error(writer, "Invalid user data", http.StatusBadRequest)
		return
	}
	exists, err := auth.RedisDB.Exists(request.Context(), user_data.Login).Result()
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
	if err = auth.RedisDB.Set(request.Context(), user_data.Login, json_user, 0).Err(); err != nil {
		http.Error(writer, fmt.Sprintf("Redis darabase error: %v", err), http.StatusInternalServerError)
		return
	}

	token, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"login": user_data.Login,
	}).SignedString(auth.PrivateKey)
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

func (auth Authenticator) Login(writer http.ResponseWriter, request *http.Request) {
	var user_data UserData
	if err := json.NewDecoder(request.Body).Decode(&user_data); err != nil {
		http.Error(writer, "Invalid user data", http.StatusBadRequest)
		return
	}
	json_user, err := auth.RedisDB.Get(request.Context(), user_data.Login).Result()
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
		fmt.Printf("Invalid login or password:\n`%v`\n`%v`\n`%v`\n`%v`\n", saved_user_data.Login, saved_user_data.Password, user_data.Login, user_data.Password)
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
	if err = auth.RedisDB.Set(request.Context(), user_data.Login, user_json, 0).Err(); err != nil {
		http.Error(writer, fmt.Sprintf("Redis darabase error: %v", err), http.StatusInternalServerError)
		return
	}

	token, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"login": user_data.Login,
	}).SignedString(auth.PrivateKey)
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

func (auth Authenticator) UpdateUserData(writer http.ResponseWriter, request *http.Request) {
	var user_data UserData
	if err := json.NewDecoder(request.Body).Decode(&user_data); err != nil {
		http.Error(writer, "Invalid user data", http.StatusBadRequest)
		return
	}
	cookie, err := request.Cookie("token")
	if err != nil {
		http.Error(writer, "User is unauthorized", http.StatusUnauthorized)
		return
	}

	claims := jwt.MapClaims{}
	_, err = jwt.ParseWithClaims(cookie.Value, claims, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, errors.New("unexpected signing method")
		}
		return auth.PublicKey, nil
	})
	if err != nil {
		http.Error(writer, "User is unauthorized", http.StatusUnauthorized)
		return
	}

	login := claims["login"].(string)
	if user_data.Login != login {
		http.Error(writer, "User is unauthorized", http.StatusUnauthorized)
		return
	}

	json_saved_user_data, err := auth.RedisDB.Get(request.Context(), login).Result()
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

	hashed_password, err := bcrypt.GenerateFromPassword([]byte(user_data.Password), bcrypt.DefaultCost)
	if err != nil {
		http.Error(writer, fmt.Sprintf("Bad password `%v` password: %v", user_data.Password, err),
			http.StatusBadRequest)
	}
	user_data.Password = string(hashed_password)
	json_user, _ := json.Marshal(user_data)

	if err = auth.RedisDB.Set(request.Context(), user_data.Login, json_user, 0).Err(); err != nil {
		http.Error(writer, fmt.Sprintf("Redis darabase error: %v", err), http.StatusInternalServerError)
		return
	}

	writer.WriteHeader(http.StatusOK)
}

func main() {
	private_key_path := flag.String("private_key_path", "", "path to private key file")
	public_key_path := flag.String("public_key_path", "", "path to public key file")
	server_port := flag.Int("port", 4200, "server port")
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

	var authenticator Authenticator
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

	if authenticator.PrivateKey, err = jwt.ParseRSAPrivateKeyFromPEM(private_key); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if authenticator.PublicKey, err = jwt.ParseRSAPublicKeyFromPEM(public_key); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	authenticator.RedisDB = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("redis:%d", *redis_port),
		Password: "",
		DB:       0,
	})

	router := mux.NewRouter()
	router.HandleFunc("/users/register", authenticator.Register).Methods("POST")
	router.HandleFunc("/users/login", authenticator.Login).Methods("POST")
	router.HandleFunc("/users/update", authenticator.UpdateUserData).Methods("PUT")

	fmt.Printf("Starting serving on port: %d\n", *server_port)

	if err = http.ListenAndServe(fmt.Sprintf(":%d", *server_port), router); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
