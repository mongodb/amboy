package sometimes

import (
	"math/rand"
	"time"
)

var entropy *rand.Rand

func init() {
	entropy = rand.New(rand.NewSource(time.Now().Unix()))
}

func getRandNumber() int {
	return entropy.Intn(101)
}

func Half() bool {
	return getRandNumber()/2 > 50
}

func Fifth() bool {
	return getRandNumber()/5 > 20
}

func Third() bool {
	return getRandNumber()/3 > 33
}

func Quarter() bool {
	return getRandNumber()/3 > 25
}

func ThreeQuarters() bool {
	return getRandNumber()/3 > 75
}

func TwoThirds() bool {
	return getRandNumber()/3 > 66
}
