# Parallel driver for IDEBench datagen.py: identical output to a serial run,
# because batch i is always seeded with seed+i. Writes one CSV per 100 batches (10M rows).
import sys, os, types, multiprocessing as mp
SRC = sys.argv[1]; OUT = sys.argv[2]; TOTAL_FILES = int(sys.argv[3]); WORKERS = int(sys.argv[4])
BATCHES_PER_FILE = 100
code = open(os.path.join(SRC, "datagen.py")).read().rsplit("DataGen()", 1)[0]
mod = types.ModuleType("datagen"); exec(compile(code, "datagen.py", "exec"), mod.__dict__)

class Gen(mod.DataGen):
    def __init__(self, file_ids):
        sys.argv = ["datagen.py", "--sample-file", f"{SRC}/data/flights/sample.csv",
                    "--sample-descriptor", f"{SRC}/data/flights/sample.json",
                    "--size", str(TOTAL_FILES * BATCHES_PER_FILE * 100000)]
        self.file_ids = file_ids
        super().__init__()
    def generate_data(self):
        orig = self.process_batch
        # run the setup part of generate_data with zero batches, then our own batches
        size = self.options.size; self.options.size = 0
        super().generate_data(); self.options.size = size
        for f in self.file_ids:
            path = f"{OUT}/part_{f:03d}.csv"
            for b in range(f * BATCHES_PER_FILE, (f + 1) * BATCHES_PER_FILE):
                self.options.output = path + ".tmp"
                orig(b)
            if f == 0:
                # batch 0 is written with a CSV header; keep all files headerless
                with open(path + ".tmp") as src, open(path + ".tmp2", "w") as dst:
                    next(src)
                    for line in src:
                        dst.write(line)
                os.rename(path + ".tmp2", path + ".tmp")
            os.rename(path + ".tmp", path)
            print("file", f, "done", flush=True)

def work(ids):
    Gen(ids)

if __name__ == "__main__":
    ids = [f for f in range(TOTAL_FILES) if not os.path.exists(f"{OUT}/part_{f:03d}.csv")]
    chunks = [ids[i::WORKERS] for i in range(WORKERS)]
    with mp.Pool(WORKERS) as p:
        p.map(work, [c for c in chunks if c], chunksize=1)
