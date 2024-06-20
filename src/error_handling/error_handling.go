package error_handling

import (
	"fmt"
	"net/http"
	"os"
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
