bin = bigsort

default:
	go build -o $(bin)

clean:
	rm -f cache/* $(bin)
