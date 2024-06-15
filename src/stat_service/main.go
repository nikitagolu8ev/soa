package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gorilla/mux"
)

type StatServer struct {
	DataBase  driver.Conn
	KafkaPort uint
}

func (server *StatServer) InitClickhouse() error {
	var err error
	server.DataBase, err = clickhouse.Open(&clickhouse.Options{
		Addr: []string{"clickhouse:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
		},
	})
	if err != nil {
		return err
	}

	// Like table
	if err = server.DataBase.Exec(context.Background(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS kafka_likes
	(
		post_id UInt64,
		user String
	) ENGINE = Kafka
	SETTINGS
		kafka_broker_list = 'kafka:%d',
		kafka_topic_list = 'like_topic',
		kafka_group_name = 'clickhouse_like_group',
		kafka_format = 'CSV'
	`, server.KafkaPort)); err != nil {
		return err
	}
	if err = server.DataBase.Exec(context.Background(), `
	CREATE TABLE IF NOT EXISTS likes
	(
		post_id UInt64,
		user String
	) ENGINE = ReplacingMergeTree
	PRIMARY KEY (post_id, user)
	`); err != nil {
		return err
	}
	if err = server.DataBase.Exec(context.Background(), `
	CREATE MATERIALIZED VIEW IF NOT EXISTS likes_view TO likes
	AS SELECT * FROM kafka_likes
	`); err != nil {
		return err
	}

	// Views table
	if err = server.DataBase.Exec(context.Background(), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS kafka_views
	(
		post_id UInt64,
		user String,
		timestamp DateTime
	) ENGINE = Kafka
	SETTINGS
		kafka_broker_list = 'kafka:%d',
		kafka_topic_list = 'view_topic',
		kafka_group_name = 'clickhouse_view_group',
		kafka_format = 'CSV'
	`, server.KafkaPort)); err != nil {
		return err
	}
	if err = server.DataBase.Exec(context.Background(), `
	CREATE TABLE IF NOT EXISTS views
	(
		post_id UInt64,
		user String,
		timestamp DateTime,
	) ENGINE = ReplacingMergeTree
	PRIMARY KEY (post_id, user)
	`); err != nil {
		return err
	}
	if err = server.DataBase.Exec(context.Background(), `
	CREATE MATERIALIZED VIEW IF NOT EXISTS views_view TO views
	AS SELECT * FROM kafka_views
	`); err != nil {
		return err
	}

	return nil
}

func (server StatServer) Ping(writer http.ResponseWriter, request *http.Request) {
	if err := server.DataBase.Ping(request.Context()); err != nil {
		http.Error(writer, fmt.Sprintf("Clickhouse error: %v", err), http.StatusInternalServerError)
	}
	writer.WriteHeader(http.StatusOK)
}

func (server StatServer) GetLikes(writer http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	post_id_str := vars["post_id"]
	post_id, err := strconv.ParseUint(post_id_str, 10, 64)
	if err != nil {
		http.Error(writer, fmt.Sprintf("Can't parse post id: %v", err), http.StatusBadRequest)
		return
	}

	var liked uint64
	rows, err := server.DataBase.Query(request.Context(), `
		SELECT * FROM likes
	`)
	if err != nil {
		http.Error(writer, fmt.Sprintf("Can't read likes table: %v", err), http.StatusBadRequest)
		return
	}
	fmt.Print("Start iterating\n")
	for rows.Next() {
		fmt.Print("Next iteration\n")
		var str string
		rows.Scan(&str)
		fmt.Printf("row: %s\n", str)
	}
	fmt.Print("Finish iterating\n")

	if err := server.DataBase.QueryRow(request.Context(), fmt.Sprintf(`
	SELECT
		count(user) AS liked
	FROM likes
	WHERE post_id == %v
	`, post_id)).Scan(&liked); err != nil {
		http.Error(writer, fmt.Sprintf("Can't check stats: %v", err), http.StatusInternalServerError)
		return
	}
	writer.Write([]byte(fmt.Sprintf("Likes: %d", liked)))
}

func main() {
	service_port := flag.Uint("service_port", 8192, "The stat server port")
	kafka_port := flag.Uint("kafka_port", 9092, "The kafka broker port")
	flag.Parse()

	server := StatServer{KafkaPort: *kafka_port}
	if err := server.InitClickhouse(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize clickhouse: %v", err)
		os.Exit(1)
		return
	}

	router := mux.NewRouter()
	router.HandleFunc("/stat/ping", server.Ping).Methods("GET")
	router.HandleFunc("/stat/likes/{post_id}", server.GetLikes).Methods("GET")

	fmt.Printf("Starting stat serving on port: %d\n", *service_port)

	if err := http.ListenAndServe(fmt.Sprintf(":%d", *service_port), router); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

}
