package error_handling

import (
	"fmt"
	"net/http"
	"os"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func CheckNonCritical(err error, msg string) bool {
	if err != nil {
		fmt.Println(msg, ":", err)
		return true
	}
	return false
}

func CheckCritical(err error, msg string) {
	if err != nil {
		fmt.Println(msg, ":", err)
		os.Exit(1)
	}
}

func CheckConditionCritical(cond bool, msg string) {
	if cond {
		fmt.Println(msg)
		os.Exit(1)
	}
}

func CheckHttp(err error, msg string, code int, writer http.ResponseWriter) bool {
	if err != nil {
		http.Error(writer, fmt.Sprint(msg, ": ", err), code)
		return true
	}
	return false
}

func CheckConditionHttp(cond bool, msg string, code int, writer http.ResponseWriter) bool {
	if cond {
		http.Error(writer, msg, code)
	}
	return cond
}

func GetHttpStatusFromGrpcStatus(code codes.Code) int {
	if code == codes.Internal {
		return http.StatusInternalServerError
	}
	return http.StatusBadRequest
}

func CheckGrpcHttp(err *status.Status, msg string, writer http.ResponseWriter) bool {
	if err != nil {
		http.Error(writer, fmt.Sprintf(msg, ": ", err.Message()), GetHttpStatusFromGrpcStatus(err.Code()))
		return true
	}
	return false
}
