package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"

	eh "error_handling"
	pb "proto"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/IBM/sarama"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type StatServer struct {
	pb.UnimplementedStatManagerServer
	DataBase driver.Conn
}

type Event struct {
	PostId   int32 `json:"post_id"`
	AuthorId int32 `json:"author_id"`
	UserId   int32 `json:"user_id"`
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

	return nil
}

func ConsumeFromKafka(db driver.Conn, topic string, db_name string) {
	kafka_addr := fmt.Sprintf("kafka:%d", 9092)
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

		insert_query := fmt.Sprintf(`INSERT INTO %s (post_id, author_id, user_id, timestamp) VALUES (%d, %d, %d, now())`,
			db_name, event.PostId, event.AuthorId, event.UserId)
		err := db.AsyncInsert(context.Background(), insert_query, true)
		if err != nil {
			fmt.Printf("Failed to insert into Clickhouse: %v\n", err)
			os.Exit(1)
		} else {
			fmt.Println("Inserted")
		}
	}
}

func (server StatServer) GetPostStats(ctx context.Context, request *pb.GetPostStatsRequest) (*pb.GetPostStatsResponse, error) {
	post_id := request.PostId
	liked := uint64(0)
	if err := server.DataBase.QueryRow(ctx, fmt.Sprintf(`SELECT count(user_id) AS liked FROM likes FINAL where post_id = %d`, post_id)).Scan(&liked); err != nil {
		return nil, status.Errorf(codes.Internal, "Can't select from stat database: %v", err)
	}
	viewed := uint64(0)
	if err := server.DataBase.QueryRow(ctx, fmt.Sprintf(`SELECT count(user_id) AS viewed FROM views FINAL where post_id = %d`, post_id)).Scan(&viewed); err != nil {
		return nil, status.Errorf(codes.Internal, "Can't select from stat database: %v", err)
	}
	return &pb.GetPostStatsResponse{PostId: post_id, Likes: uint32(liked), Views: uint32(viewed)}, nil
}

func (server StatServer) GetTopPosts(ctx context.Context, request *pb.GetTopPostsRequest) (*pb.GetTopPostsResponse, error) {
	var rows driver.Rows
	var err error
	if request.OrderBy == pb.OrderPostsBy_LIKES {
		query := `
		SELECT
			post_id,
			any(author_id) AS author_id,
			count(user_id) AS liked
		FROM likes
		FINAL
		GROUP BY post_id
		ORDER BY liked DESC
		LIMIT 5`
		rows, err = server.DataBase.Query(ctx, query)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Can't select from stat database: %v", err)
		}
	} else {
		query := `
		SELECT
			post_id,
			any(author_id) AS author_id,
			count(user_id) AS liked
		FROM views
		FINAL
		GROUP BY post_id
		ORDER BY liked DESC
		LIMIT 5`
		rows, err = server.DataBase.Query(ctx, query)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Can't select from stat database: %v", err)
		}
	}
	defer rows.Close()

	var posts []*pb.PostStats
	for rows.Next() {
		var post pb.PostStats
		var stat uint64
		if err = rows.Scan(&post.PostId, &post.AuthorId, &stat); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to iterate stats top results: %s", err)
		}
		post.Stat = uint32(stat)
		posts = append(posts, &post)
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to iterate stats top results: %s", err)
	}
	return &pb.GetTopPostsResponse{PostStats: posts}, nil
}

func (server StatServer) GetTopAuthors(ctx context.Context, _ *emptypb.Empty) (*pb.GetTopAuthorsResponse, error) {
	query := `
	SELECT
		author_id,
		count(user_id) AS liked
	FROM likes
	FINAL
	GROUP BY author_id
	ORDER BY liked DESC
	LIMIT 3`
	rows, err := server.DataBase.Query(ctx, query)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Can't select from stat database: %v", err)
	}
	defer rows.Close()

	var authors []*pb.AuthorStats
	for rows.Next() {
		var author pb.AuthorStats
		var likes uint64
		if err = rows.Scan(&author.AuthorId, &likes); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to iterate stats top results: %s", err)
		}
		author.Likes = uint32(likes)
		authors = append(authors, &author)
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to iterate stats top results: %s", err)
	}
	return &pb.GetTopAuthorsResponse{AuthorStats: authors}, nil
}

func (server StatServer) DeletePost(ctx context.Context, request *pb.DeletePostRequest) (*emptypb.Empty, error) {
	like_query := fmt.Sprintf(`DELETE FROM likes WHERE post_id = %d AND author_id = %d`, request.PostId, request.AuthorId)
	view_query := fmt.Sprintf(`DELETE FROM views WHERE post_id = %d AND author_id = %d`, request.PostId, request.AuthorId)

	fmt.Println(like_query)
	fmt.Println(view_query)

	if err := server.DataBase.Exec(ctx, like_query); err != nil {
		return nil, status.Errorf(codes.Internal, "Can't delete from database: %v", err)
	}
	if err := server.DataBase.Exec(ctx, view_query); err != nil {
		return nil, status.Errorf(codes.Internal, "Can't delete from database: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func main() {
	service_port := flag.Uint("service_port", 8192, "The stat server port")
	// kafka_port := flag.Uint("kafka_port", 9092, "The kafka broker port")
	flag.Parse()

	db, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"clickhouse:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
		},
	})
	eh.CheckCritical(err, "Couldn't open clickhouse database")
	eh.CheckCritical(db.Ping(context.Background()), "Couldn't reach clickhouse database")

	go ConsumeFromKafka(db, "like_topic", "likes")
	go ConsumeFromKafka(db, "view_topic", "views")

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *service_port))
	eh.CheckCritical(err, "Failed to listen")

	stat_server := grpc.NewServer()
	pb.RegisterStatManagerServer(stat_server, &StatServer{DataBase: db})

	fmt.Printf("Starting stat serving on port: %d\n", *service_port)
	eh.CheckCritical(stat_server.Serve(lis), "stat_service")

	// router := mux.NewRouter()
	// router.HandleFunc("/stat/ping", server.Ping).Methods("GET")
	// router.HandleFunc("/stat/likes/{post_id}", server.GetLikes).Methods("GET")

	// fmt.Printf("Starting stat serving on port: %d\n", *service_port)
	// eh.CheckCritical(http.ListenAndServe(fmt.Sprintf(":%d", *service_port), router), "stat_service")
}
