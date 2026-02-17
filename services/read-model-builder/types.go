package main

import "time"

type Entry struct {
	AccountID string `json:"account_id"`
	Direction string `json:"direction"`
	Amount    int64  `json:"amount"`
}

type TransactionPostedEvent struct {
	EventID       string  `json:"event_id"`
	TransactionID string  `json:"transaction_id"`
	Entries       []Entry `json:"entries"`
	Status        string  `json:"status"`
	OccurredAt    string  `json:"occurred_at"`
	Version       int     `json:"version"`
}


func (e TransactionPostedEvent) OccurredAtTime() time.Time {
	t, _ := time.Parse(time.RFC3339Nano, e.OccurredAt)
	return t
}

func (e TransactionPostedEvent) TotalAmount() int64 {
	var total int64
	for _, it := range e.Entries {
		if it.Direction == "debit" {
			total += it.Amount
		}
	}
	return total
}
