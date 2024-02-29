package KafkaLog

import (
	"github.com/golovin-a/KafkaLog/tools/kafka"
)

func HandleCustomError(eh *kafka.Config, errorJSON []byte) {
	eh.Send(errorJSON)
}
