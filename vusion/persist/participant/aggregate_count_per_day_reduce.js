function(k, vals) { 
    var numOfTrue = 0;
    var numOfFalse = 0;
    for(var i=0; i < vals.length; i++){
        if (vals[i] === true) {
            numOfTrue++;
        } else {
            numOfFalse++;
        }
    }
    return {'opt-in': numOfTrue, 'opt-out': numOfFalse}; 
}