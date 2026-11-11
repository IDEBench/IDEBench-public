# Summarize IDEBench detailed reports (reports/*.csv) the way the paper's summary report does:
# mean TR-violation %, mean missing bins, and MRE statistics over queries that did not violate TR.
# Usage: summarize.py <reports_dir> [comma-separated group-by columns] [workflow-type filter]
import csv, glob, sys, statistics
from collections import defaultdict

rows = []
for f in glob.glob(sys.argv[1] + "/*.csv"):
    rows.extend(csv.DictReader(open(f)))

group_by = sys.argv[2].split(",") if len(sys.argv) > 2 and sys.argv[2] else ["interface", "size", "time_requirement"]
wfilter = sys.argv[3] if len(sys.argv) > 3 else None

groups = defaultdict(list)
for r in rows:
    r["wtype"] = r["workflow"].rsplit("_", 1)[0]
    if wfilter and r["wtype"] != wfilter:
        continue
    groups[tuple(r[k] for k in group_by)].append(r)

def size_key(s):
    return float(s[:-1]) * {"k": 1e3, "m": 1e6, "b": 1e9}[s[-1].lower()]

def sort_key(k):
    return tuple(size_key(v) if g == "size" else (float(v) if g in ("time_requirement", "think_time") else v)
                 for g, v in zip(group_by, k))

hdr = group_by + ["queries", "workflows", "viol %", "missing bins %", "MRE mean %", "MRE median %",
                  "MRE p95 %", "MRE area %", "p50 ms", "p95 ms", "max ms"]
print("| " + " | ".join(hdr) + " |")
print("|" + "---|" * len(hdr))
for k in sorted(groups, key=sort_key):
    g = groups[k]
    ok = [r for r in g if r["time_violated"] != "True"]
    viol = 100.0 * (len(g) - len(ok)) / len(g)
    missing = 100.0 * statistics.mean(float(r["missing_bins"]) for r in g)
    mre = sorted(float(r["rel_error_avg"]) for r in ok)
    durs = sorted(int(r["duration"]) for r in g)
    if mre:
        # area above the MRE CDF truncated at 100%, as in the paper's summary report
        area = 100.0 * statistics.mean(min(m, 1.0) for m in mre)
        stats = ["%.2f" % (100 * statistics.mean(mre)), "%.2f" % (100 * statistics.median(mre)),
                 "%.2f" % (100 * mre[int(0.95 * (len(mre) - 1))]), "%.2f" % area]
    else:
        stats = ["-"] * 4
    print("| " + " | ".join(list(k) + [str(len(g)), str(len({r["workflow"] for r in g})),
                                       "%.1f" % viol, "%.1f" % missing] + stats +
                            [str(durs[len(durs) // 2]), str(durs[int(0.95 * (len(durs) - 1))]), str(durs[-1])]) + " |")
