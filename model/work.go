package model

import "github.com/google/uuid"

type Work struct {
	ID   uint64
	GID  uuid.UUID
	Name string `boltholdIndex:"Name"`
	Lang string
}
