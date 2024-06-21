package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"

	eh "error_handling"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/IBM/sarama"
	"github.com/gorilla/mux"
)

type StatServer struct {
	DataBase  driver.Conn
	KafkaPort uint
}

type Event struct {
	PostId int32 `json:"post_id"`
	UserId int32 `json:"user_id"`
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
	// if err = server.DataBase.Exec(context.Background(), `
	// CREATE TABLE IF NOT EXISTS kafka_likes
	// (
	// 	post_id UInt64,
	// 	user String
	// ) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka:9092',
	//  						  kafka_topic_list = 'like_topic',
	// 						  kafka_group_name = 'ch_like_group',
	// 						  kafka_format = 'CSV'
	// `); err != nil {
	// 	return err
	// }

	// if err = server.DataBase.Exec(context.Background(), `
	// CREATE TABLE IF NOT EXISTS likes
	// (
	// 	post_id Int32,
	// 	user Int32,
	// 	timestamp DateTime,
	// ) ENGINE = ReplacingMergeTree
	// PRIMARY KEY (post_id, user)
	// `); err != nil {
	// 	return err
	// }

	// if err = server.DataBase.Exec(context.Background(), `
	// CREATE MATERIALIZED VIEW IF NOT EXISTS likes_consumer TO likes
	// 	AS SELECT post_id, user, now() as timestamp from kafka_likes
	// `); err != nil {
	// 	return err
	// }

	// View table
	// if err = server.DataBase.Exec(context.Background(), `
	// CREATE TABLE IF NOT EXISTS views
	// (
	// 	post_id Int32,
	// 	user Int32,
	// 	timestamp DateTime,
	// ) ENGINE = ReplacingMergeTree
	// PRIMARY KEY (post_id, user)
	// `); err != nil {
	// 	return err
	// }

	return nil
}

func (server StatServer) ConsumeFromKafka(topic string, db_name string) {
	kafka_addr := fmt.Sprintf("kafka:%d", server.KafkaPort)
	consumer, err := sarama.NewConsumer([]string{kafka_addr}, nil)
	eh.CheckCritical(err, "Failed to create kafka consumer")
	defer func() {
		eh.CheckCritical(consumer.Close(), "Failed to close kafka consumer")
	}()

	partition_consumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	eh.CheckCritical(err, "Failed to create kafka partition consumer")
	defer func() {
		eh.CheckCritical(err, "Failed to create kafka partition consumer")
		if err = partition_consumer.Close(); err != nil {
			fmt.Printf("Failed to close kafka partition consumer: %v\n", err)
			os.Exit(1)
		}
	}()

	for {
		msg := <-partition_consumer.Messages()
		var event Event
		if err = json.Unmarshal(msg.Value, &event); err != nil {
			fmt.Printf("Failed to unmarshal kafka message: %v\n", err)
			continue
		}

		insert_query := fmt.Sprintf(`INSERT INTO %s (post_id, user, timestamp) VALUES (%d, %d, now())`, db_name, event.PostId, event.UserId)
		err := server.DataBase.AsyncInsert(context.Background(), insert_query, true)
		if err != nil {
			fmt.Printf("Failed to insert into Clickhouse: %v\n", err)
			os.Exit(1)
		} else {
			fmt.Println("Inserted")
		}
	}
}

func (server StatServer) Ping(writer http.ResponseWriter, request *http.Request) {
	if eh.CheckHttp(server.DataBase.Ping(request.Context()), "Clickhouse error", http.StatusInternalServerError, writer) {
		return
	}
	writer.WriteHeader(http.StatusOK)
}

func (server StatServer) GetLikes(writer http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	post_id_str := vars["post_id"]
	post_id, err := strconv.ParseInt(post_id_str, 10, 32)
	if err != nil {
		http.Error(writer, fmt.Sprintf("Can't parse post id: %v", err), http.StatusBadRequest)
		return
	}

	var liked uint64

	select_query := fmt.Sprintf(`SELECT count(user) AS liked FROM likes FINAL WHERE post_id = %d`, post_id)
	if eh.CheckHttp(server.DataBase.QueryRow(request.Context(), select_query).Scan(&liked), "Can't check stats", http.StatusInternalServerError, writer) {
		return
	}
	writer.Write([]byte(fmt.Sprintf("Likes: %d", liked)))
}

func main() {
	service_port := flag.Uint("service_port", 8192, "The stat server port")
	kafka_port := flag.Uint("kafka_port", 9092, "The kafka broker port")
	flag.Parse()

	server := StatServer{KafkaPort: *kafka_port}
	eh.CheckCritical(server.InitClickhouse(), "Failed to initialize clickhouse")

	go server.ConsumeFromKafka("like_topic", "likes")
	go server.ConsumeFromKafka("view_topic", "views")

	router := mux.NewRouter()
	router.HandleFunc("/stat/ping", server.Ping).Methods("GET")
	router.HandleFunc("/stat/likes/{post_id}", server.GetLikes).Methods("GET")

	fmt.Printf("Starting stat serving on port: %d\n", *service_port)
	eh.CheckCritical(http.ListenAndServe(fmt.Sprintf(":%d", *service_port), router), "stat_service")
}
