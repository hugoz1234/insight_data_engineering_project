var NYC = new google.maps.LatLng(40.714185, -74.003256);
var markers = [];
var map;

function initialize() { 
	var mapOptions = {
		zoom: 12,
		center: NYC
    };
    map = new google.maps.Map(document.getElementById('map-canvas'),
			      mapOptions);
    update_values();
}

function update_values() {
	var no_data = false;
	$.getJSON('/get_realtime_businesses',
              function(data) {
                businesses = data.business_data;
                if (businesses.length === 0 || businesses === undefined) {
                	no_data = true;
                }
				clearMarkers();
				for (var key in businesses) {
					lat = businesses[key]['latitude'];
					lng = businesses[key]['longitude'];
					b_id = businesses[key]['business_id'];
					name = businesses[key]['name']
				    addMarker(new google.maps.LatLng(lat, lng), b_id, name);
				}
				});
	if (no_data){
		update_values();
	}else {
    	window.setTimeout(update_values, 30000);
	}
}

function addMarker(position, b_id, name) {
	var marker = new google.maps.Marker({
		position: position,
		map: map,
		title: 'Click to analyze ' + name,
    });
    marker.metadata = {'b_id': b_id, 'name': name}

    marker.addListener('click', function(){
    	display_timeseries(marker.metadata['b_id'], marker.metadata['name']);
    });

    markers.push(marker);
}


function clearMarkers() {
    for (var i = 0; i < markers.length; i++) {
		markers[i].setMap(null);
    }
    markers = [];
}
google.maps.event.addDomListener(window, 'load', initialize);