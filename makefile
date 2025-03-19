NT := 10000
input_file := numbers.txt
output_file := sorted_numbers.txt

all: qs qsp

qs:
	gcc -Wall -Wextra -fopenmp -pthread -o qs qs.c

qso:
	gcc -Wall -Wextra -O3 -ffast-math -march=native -fopt-info-vec -fopenmp -pthread -o qs qs.c

qsp:
	gcc -Wall -Wextra -fopenmp -pthread -o qsp qsp.c

qsoo:
	gcc -Wall -Wextra -O3 -ffast-math -march=native -fopt-info-vec -fopenmp -pthread -o qsp qsp.c

run: qs
	./qs $(NT) $(input_file) $(output_file)

clean:
	rm -f qs qsp
