function() {
    var optinDate = new Date(Date.parse(%s));
    var endPeriode;
    var today = new Date(Date.now());
    if (this["last-optout-date"] != null) {
        endPeriode = new Date(this["last-optout-date"].substring(0,10));
    } else {
        endPeriode = today;
    }

    function dateFormat(d) {
        var yyyy = d.getFullYear().toString();
        var mm = (d.getMonth()+1).toString();
        var dd  = d.getDate().toString(); 
        return yyyy + "-" + (mm[1]?mm:"0"+mm[0]) + "-" + (dd[1]?dd:"0"+dd[0]);
    }
    print("start date "+ optinDate);
    print("end date"+ endPeriode);
    for (var d = optinDate; d <= endPeriode; d.setDate(d.getDate() + 1)) {
        current = dateFormat(d);
        emit(current, true); 
        print("emit one " + d)
    }
    endPeriode.setDate(endPeriode.getDate() + 1);
    if (today == endPeriode) {
        return;
    }
    for (var d = endPeriode; d <= today; d.setDate(d.getDate() + 1)) {
        current = dateFormat(d);
        emit(current, false);   
    }
}