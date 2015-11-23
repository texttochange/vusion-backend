function(k, vals) {
    var reducedDay = {
        'opt-in': 0,
        'opt-out': 0}
    vals.forEach(function(value) {
        reducedDay['opt-in'] += value['opt-in'];
        reducedDay['opt-out'] += value['opt-out'];
        });
    return reducedDay;
}