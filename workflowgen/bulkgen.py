import os

t = "inverse_bushy"
config = "config/%s.json" % t
sizes = [25]
for size in sizes:
    for n in range(10):
        seed = 1000 * size + n
        os.system("python workflowgen.py -n %i -r %i --config %s -p %s" % (size, seed, config, ("../workflows/%s_%i_r%i.json" % (t, size,seed))))