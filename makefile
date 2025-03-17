NT := 10000
input_file := numbers.txt
output_file := sorted_numbers.txt

all: qs

qs:
	gcc -Wall -Wextra -fopenmp -pthread -o qs qs.c

qso:
	gcc -Wall -Wextra -O3 -ffast-math -march=native -fopt-info-vec -fopenmp -pthread -o qs qs.c

run: qs
	./qs $(NT) $(input_file) $(output_file)

clean:
	rm -f qs
