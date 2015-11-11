function() {
    var from = "%s";
    var today = new Date(Date.parse("%s"));
    var optinDate = new Date(Date.parse(this["last-optin-date"].substring(0,10)));
    var optoutDate = null;
    if (this["last-optout-date"] != null) {
        optoutDate = new Date(this["last-optout-date"].substring(0,10));
    } 
    var startPeriode = optinDate;
    if (from != "") {
        startPeriode = new Date(Date.parse(from));
    }

    function dateFormat(d) {
        var yyyy = d.getFullYear().toString();
        var mm = (d.getMonth()+1).toString();
        var dd  = d.getDate().toString(); 
        return yyyy + "-" + (mm[1]?mm:"0"+mm[0]) + "-" + (dd[1]?dd:"0"+dd[0]);
    }
    var runningDate = new Date(startPeriode);
    while (true) {
        if ((optoutDate != null && runningDate > optoutDate) || runningDate > today) {
            break;
        }
        current = dateFormat(runningDate);
        emit(current, true); 
        runningDate.setDate(runningDate.getDate() + 1)
    }
    while (true) {
        if (runningDate > today) {
            break;
        }
        current = dateFormat(runningDate);
        emit(current, false);
        runningDate.setDate(runningDate.getDate() + 1)
    }
}