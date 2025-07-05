# DEBUG MEMORY LEAK
import tracemalloc


# DEBUG MEMORY LEAK
tracemalloc.start()
snapshot1 = tracemalloc.take_snapshot()

# DEBUG MEMORY LEAK
snapshot2 = tracemalloc.take_snapshot()
top_stats = snapshot2.compare_to(snapshot1, 'lineno')
memory_file = os.path.join(os.path.dirname(cancel_file),f'debug-00m-{ticker}.txt')
with open(memory_file,'w') as f:
    f.write("[ Top 10 differences ]\n")
    for stat in top_stats[:10]:
        f.write(f"{str(stat)}\n")

