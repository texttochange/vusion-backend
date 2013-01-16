import io, json
from utils import ObjectMaker

maker = ObjectMaker()
with io.open('generated_participants.json', 'wb') as f:
    for i in range(1, 5000000):
        phone = "+256"+ str(i) 
        p = maker.mkobj_participant_v2(participant_phone=phone)
        json.dump(p, f)
        f.write("\n")
        
    
    