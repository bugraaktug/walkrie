import random
import requests

BASE = "http://localhost:6333"
COLL = "walkrie_manual_test"
DIM = 1024  # matches the BGE-M3 setup used elsewhere this session

random.seed(42)


def rand_vec():
    return [random.uniform(-1, 1) for _ in range(DIM)]


def perturb(vec, amount=0.05):
    return [v + random.uniform(-amount, amount) for v in vec]


# 1. (re)create collection
requests.delete(f"{BASE}/collections/{COLL}")
r = requests.put(
    f"{BASE}/collections/{COLL}",
    json={"vectors": {"size": DIM, "distance": "Cosine"}},
)
print("create collection:", r.status_code, r.json())

# 2. insert points — point 2 is a near-duplicate of point 1, point 3 is unrelated
base = rand_vec()
points = [
    {"id": 1, "vector": base, "payload": {"item_id": "doc-1", "text": "postgres logical replication CDC"}},
    {"id": 2, "vector": perturb(base), "payload": {"item_id": "doc-2", "text": "logical replication in postgres, near duplicate of doc-1"}},
    {"id": 3, "vector": rand_vec(), "payload": {"item_id": "doc-3", "text": "completely unrelated: sourdough bread recipe"}},
]
r = requests.put(f"{BASE}/collections/{COLL}/points", json={"points": points})
print("upsert points:", r.status_code, r.json())

# 3. search with a query vector close to point 1 — expect [1, 2, 3] order (1 or 2 first)
query = perturb(base, amount=0.02)
r = requests.post(
    f"{BASE}/collections/{COLL}/points/search",
    json={"vector": query, "limit": 3, "with_payload": True},
)
print("\nsearch results (expect doc-1/doc-2 top, doc-3 last):")
for hit in r.json()["result"]:
    print(f"  id={hit['id']} score={hit['score']:.4f} payload={hit['payload']}")
