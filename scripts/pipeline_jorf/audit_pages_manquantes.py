import json, os, sys
root = sys.argv[1]
items = json.load(open(sys.argv[2]))["items"]
arks = ["bpt6k2093238x","bpt6k2093239b","bpt6k20934020","bpt6k20932593","bpt6k20932400",
        "bpt6k2093378s","bpt6k2093403d","bpt6k2093341r","bpt6k2093258p","bpt6k20932185",
        "bpt6k20933796","bpt6k2093354c"]
print("%-34s %7s %8s  %-16s %s" % ("numero", "disque", "attendu", "alternative", "etat"))
tot = 0
for it in items:
    nid = it["numero_id"]
    if not any(a in nid for a in arks):
        continue
    d = os.path.join(root, it["revue"], nid)
    n = len([f for f in os.listdir(d) if f.endswith(".jpg")]) if os.path.isdir(d) else 0
    alt = [a for a in it.get("arks_alternatifs", []) if a.get("pages") == it["pages_total"]]
    manque = it["pages_total"] - n
    tot += max(manque, 0)
    print("%-34s %7d %8d  %-16s %s" % (
        nid, n, it["pages_total"],
        alt[0]["ark"] if alt else "AUCUNE",
        "OK" if manque <= 0 else "-%d" % manque))
print("\ntotal pages manquantes sur ces 12 : %d" % tot)
