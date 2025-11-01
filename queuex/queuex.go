package queuex

type Marshaler interface {
	Marshal() (string, error)
	Unmarshal(string) error
}
