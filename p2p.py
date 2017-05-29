import sys
import json
import time
import threading
import hashlib
import random
import requests
from bottle import route, run, request

knowledge = {}
known_roots = set()
known_peers = {}
knowledge_timehash = {}

@route('/test')
def test():
    return "Hello World!"

@route('/sync', method='post')
def sync():
    data = json.load(request.body)
    name = data['name']
    print 'contacted by', name
    known_peers[name] = time.time()
    other_knowledge_timehash = data['knowledge_timehash']
    keys_to_send = []
    for key in other_knowledge_timehash:
        if (key in knowledge_timehash and
            knowledge_timehash[key][0] > other_knowledge_timehash[key][0] and
            knowledge_timehash[key][1] != other_knowledge_timehash[key][1]):
            keys_to_send.append(key)
    for key in knowledge_timehash:
        if not key in other_knowledge_timehash:            
            keys_to_send.append(key)
    knowledge_to_send = {key: knowledge[key] for key in keys_to_send}
    return json.dumps(knowledge_to_send)

def discover(func, name):
    while True:
        dt = func()
        for key in knowledge:
            jinfo = json.dumps(knowledge[key], sort_keys=True)
            th = hashlib.sha1(jinfo).hexdigest()
            if not key in knowledge_timehash or knowledge_timehash[key][1]!=th:
                knowledge_timehash[key] = [time.time(), th]
        for pname in known_peers:
            if pname in known_roots or time.time() - known_peers[pname] < 60:
                data = {'name':name, 'knowledge_timehash':knowledge_timehash}                
                try:
                    res = requests.post(pname+'/sync', json=data, timeout=2)
                except requests.ConnectionError:
                    continue
                if res.status_code == 200:
                    knowledge_received = res.json()
                    for key in knowledge_received:
                        if key.endswith('_merge') and isinstance(knowledge[key], dict):
                            knowledge[key].update(knowledge_received[key])
                        elif key.endswith('_union') and isinstance(knowledge[key], list):
                            knowledge[key] = list(set(knowledge[key])|set(knowledge_received[key]))
                        else:
                            knowledge[key] = knowledge_received[key]
        time.sleep(dt)

def main(func, name, *roots):
    for root in roots: 
        known_roots.add(root)
        known_peers[root] = 0

    thread = threading.Thread(target = discover, args=(func, name)).start()
    try:
        port = int(name.split('/')[2].split(':')[1])
        print 'binding to port', port
        run(host='localhost', port=port, debug=True)
    except KeyboardInterrupt:    
        thread.join()
        sys.exit()

def test_func():
    if random.random()<0.01:
        knowledge['test'] = random.random()
    if not 'test' in knowledge:
        knowledge['test'] = random.random()
    print knowledge, known_peers, known_roots
    return random.random()

if __name__ == '__main__':
    main(test_func, sys.argv[1], *sys.argv[2:])
