package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"net"

	eh "error_handling"
	pb "proto"

	_ "github.com/lib/pq"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type PostServer struct {
	pb.UnimplementedPostManagerServer
	DataBase     *sql.DB
	PostsPerPage uint
}

func (server *PostServer) CreatePost(ctx context.Context, request *pb.CreatePostRequest) (*pb.CreatePostResponse, error) {
	var post_id uint64
	if err := server.DataBase.QueryRowContext(
		ctx,
		"INSERT INTO posts (title, author, content) VALUES ($1, $2, $3) RETURNING post_id",
		request.Title,
		request.Author,
		request.Content,
	).Scan(&post_id); err != nil {
		return nil, fmt.Errorf("failed to create post: %s", err)
	}
	return &pb.CreatePostResponse{PostId: &post_id}, nil
}

func (server *PostServer) UpdatePost(ctx context.Context, request *pb.UpdatePostRequest) (*pb.SuccessResponse, error) {
	exec, err := server.DataBase.ExecContext(
		ctx,
		"UPDATE posts SET title = $1, content = $2 WHERE post_id = $3 AND author = $4",
		request.Title,
		request.Content,
		request.PostId,
		request.Author,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update post: %v", err)
	}
	rows, err := exec.RowsAffected()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update post: %v", err)
	}
	if rows == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "no post with id: %d", request.PostId)
	}
	return &pb.SuccessResponse{Successful: true}, nil
}

func (server *PostServer) DeletePost(ctx context.Context, request *pb.DeletePostRequest) (*pb.SuccessResponse, error) {
	exec, err := server.DataBase.ExecContext(
		ctx,
		"DELETE FROM posts WHERE post_id = $1 AND author = $2",
		request.PostId,
		request.Author,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete post: %v", err)
	}
	rows, err := exec.RowsAffected()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete post: %v", err)
	}
	if rows == 0 {
		return &pb.SuccessResponse{Successful: false}, nil
	}
	return &pb.SuccessResponse{Successful: true}, nil
}

func (server *PostServer) GetPostById(ctx context.Context, request *pb.GetPostByIdRequest) (*pb.GetPostByIdResponse, error) {
	var post pb.Post
	if err := server.DataBase.QueryRowContext(
		ctx,
		"SELECT post_id, title, author, content FROM posts WHERE post_id = $1",
		request.PostId,
	).Scan(&post.PostId, &post.Title, &post.Author, &post.Content); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to get post: %s", err)
	}
	return &pb.GetPostByIdResponse{Post: &post}, nil
}

func (server *PostServer) GetPostsOnPage(ctx context.Context, request *pb.GetPostsOnPageRequest) (*pb.GetPostsOnPageResponse, error) {
	var posts []*pb.Post
	rows, err := server.DataBase.QueryContext(
		ctx,
		"SELECT post_id, title, author, content FROM posts LIMIT $1 OFFSET $2",
		server.PostsPerPage,
		server.PostsPerPage*uint(request.PageId),
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get posts page: %s", err)
	}
	defer rows.Close()

	for rows.Next() {
		var post pb.Post
		if err = rows.Scan(&post.PostId, &post.Title, &post.Author, &post.Content); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to iterate posts page: %s", err)
		}
		posts = append(posts, &post)
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to iterate posts page: %s", err)
	}
	return &pb.GetPostsOnPageResponse{Posts: posts}, nil
}

func main() {
	port := flag.Int("port", 50051, "The post server port")
	posts_per_page := flag.Uint("posts_per_page", 10, "The maximum amount of posts on one page")
	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	eh.CheckCritical(err, "Failed to listen")

	db, err := sql.Open("postgres", "host=posts_postgres port=5432 user=post_service password=password dbname=posts_db sslmode=disable")
	eh.CheckCritical(err, "Can't open post service database")
	defer db.Close()

	eh.CheckCritical(db.Ping(), "Can't reach post service database")

	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS posts (
		post_id SERIAL PRIMARY KEY,
		title TEXT,
		author TEXT,
		content TEXT
	)
	`)
	eh.CheckCritical(err, "Can't create posts table")

	post_server := grpc.NewServer()
	pb.RegisterPostManagerServer(post_server, &PostServer{DataBase: db, PostsPerPage: *posts_per_page})

	fmt.Printf("Starting post serving on port: %d\n", *port)
	eh.CheckCritical(post_server.Serve(lis), "post_service")
}
