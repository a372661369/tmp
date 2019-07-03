func consumer(msgChan chan amqp.Delivery, mq *SenderMq) {
	conn, channel := mqConnect(mq.SENDERURL)
	defer func() {
		_ = channel.Close()
		_ = conn.Close()
	}()
	err := channel.ExchangeDeclare(mq.SENDEREXCHANGE, "topic", true, false, false, false, nil)
	fail := failOnErr(err, "exchange declare error")
	if fail != nil {
		return
	}
	fail = declareQueue(channel, mq.SENDERQUEUE)
	if fail != nil {
		return
	}
	err = channel.QueueBind(mq.SENDERQUEUE, mq.SENDEROUTINGKEY, mq.SENDEREXCHANGE, false, nil)
	fail = failOnErr(err, "queue bind error")
	if fail != nil {
		return
	}
	for msg := range msgChan {

		err = channel.Publish(mq.SENDEREXCHANGE, mq.SENDEROUTINGKEY, false, false,
			amqp.Publishing{ContentType:msg.ContentType, ContentEncoding: msg.ContentType,
				DeliveryMode: msg.DeliveryMode, Body: msg.Body})
		fail = failOnErr(err, "publish msg error")
		if fail != nil {
			_ = msg.Reject(true)
			return
		}
		log.Info("publish msg ok")
		_ = msg.Ack(false)
	}
	return
}

func declareQueue(channel *amqp.Channel, queueName string) error{
	_, err := channel.QueueDeclare(queueName, true, false, false, true, amqp.Table{"x-max-priority": 10})
	return failOnErr(err, "Fail on create queue")
}

func do(receiverMq *ReceiverMq, senderMq *SenderMq) error{
	//conn, channel := mqConnect(receiverMq.RECEIVERURL)
	conn, err :=amqp.Dial(receiverMq.RECEIVERURL)
	fail := failOnErr(err, "connect url error")
	if fail != nil {
		return fail
	}
	channel , err := conn.Channel()
	fail = failOnErr(err, "channel error")
	if fail != nil {
		return fail
	}
	log.Info("connect ok")

	_ = channel.Qos(3, 0, false)
	fail = declareQueue(channel, receiverMq.RECEIVERQUEUE)
	if fail != nil {
		return fail
	}
	err = channel.ExchangeDeclare(receiverMq.RECEIVEREXCHANGE, "topic", true, false, false, false, nil)
	fail = failOnErr(err, "exchange declare error")
	if fail != nil {
		return fail
	}
	err = channel.QueueBind(receiverMq.RECEIVERQUEUE, receiverMq.RECEIVEROUTINGKEY, receiverMq.RECEIVEREXCHANGE, false, nil)
	fail = failOnErr(err, "queue bind error")
	if fail != nil {
		return fail
	}
	defer func() {
		_ = channel.Close()
	}()
	defer func() {
		_ = conn.Close()
	}()
	msgChan, err := channel.Consume(receiverMq.RECEIVERQUEUE, "GO_transfer_consumer", false, false, false, false, nil)
	fail = failOnErr(err, "fail to get msg")
	if fail != nil {
		return fail
	}
	consumerChan := make(chan amqp.Delivery, 3)
	for i := 0; i < 3; i++ {
		go func() {
			consumer(consumerChan, senderMq)
		}()
	}
	for msg := range msgChan {
		log.Info("get message %s", bytes.NewBuffer(msg.Body))
		consumerChan <- msg
	}
	return nil
}
