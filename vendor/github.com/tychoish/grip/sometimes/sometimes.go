package sometimes

import (
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().Unix())
}

func getRandNumber() int {
	return rand.Intn(101)
}

func Fifth() bool {
	return getRandNumber() > 80
}

func Half() bool {
	return getRandNumber() > 50
}

func Third() bool {
	return getRandNumber() > 66
}

func Quarter() bool {
	return getRandNumber() > 75
}

func ThreeQuarters() bool {
	return getRandNumber() > 25
}

func TwoThirds() bool {
	return getRandNumber() > 33
}
