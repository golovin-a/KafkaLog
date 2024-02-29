package KafkaLog

import (
	"encoding/json"
	"github.com/golovin-a/KafkaLog/tools/kafka"
	"net/http"
	"runtime"
	"strconv"
	"time"
)

type Error struct {
	File          string `json:"file"`
	Message       string `json:"message"`
	RequestUrl    string `json:"request_url"`
	RequestMethod string `json:"request_method"`
	ServiceName   string `json:"service_name"`
	SessionId     string `json:"session_id"`
	ShopName      string `json:"shop_name"`
	Timestamp     int64  `json:"timestamp"`
	UserAgent     string `json:"user_agent"`
	UserIp        string `json:"user_ip"`
	UserRegion    string `json:"user_region"`
}

func HandleError(eh *kafka.Config, err error, r *http.Request) {
	_, filename, line, _ := runtime.Caller(1)
	file := filename + ":" + strconv.Itoa(line)
	errorData := getLogData(err, r, file, *eh.ServiceName)
	errorJSON, _ := json.Marshal(errorData)
	eh.Send(errorJSON)
}

func getLogData(err error, r *http.Request, file string, serviceName string) *Error {
	remoteAddr := r.RemoteAddr
	userRegion := "Москва"
	shopName := ""
	userAgent := r.UserAgent()
	sessionId := ""
	if r.Header.Get("Http_x_real_ip") != "" {
		remoteAddr = r.Header.Get("Http_x_real_ip")
	}
	if r.Header.Get("User_region") != "" {
		userRegion = r.Header.Get("User_region")
	}
	if r.Header.Get("Shop_name") != "" {
		shopName = r.Header.Get("Shop_name")
	}
	if r.Header.Get("User_agent") != "" {
		userAgent = r.Header.Get("User_agent")
	}
	if r.Header.Get("Session_id") != "" {
		sessionId = r.Header.Get("Session_id")
	}
	return &Error{
		File:          file,
		Message:       err.Error(),
		RequestUrl:    r.URL.Path,
		RequestMethod: r.Method,
		ServiceName:   serviceName,
		SessionId:     sessionId,
		ShopName:      shopName,
		Timestamp:     time.Now().Unix(),
		UserAgent:     userAgent,
		UserIp:        remoteAddr,
		UserRegion:    userRegion,
	}
}
