package events

type OrderPlacedEvent struct {
	OrderID   string `json:"orderId"`
	ProductID string `json:"productId"`
	Quantity  int    `json:"quantity"`
}

type InventoryStatusEvent struct {
	OrderID string `json:"orderId"`
	Status  string `json:"status"` // RESERVED, REJECTED
	Reason  string `json:"reason,omitempty"`
}
