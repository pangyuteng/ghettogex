

https://stackoverflow.com/questions/582336/how-do-i-profile-a-python-script

https://github.com/psycopg/psycopg/issues/1101


python -m cProfile -o program.prof pg_profiling.py ok

docker build -t snakeviz:latest .

docker run -it -p 5000:5000 -w /workdir -v $PWD:/workdir snakeviz:latest \
    bash -c "snakeviz -p 5000 --hostname="*" --server program.prof"

http://192.168.68.143:5000/snakeviz/