if (location.hostname === "localhost") {
	const loaded = new Date().toUTCString();
	async function check() {
		const res = await fetch("/app.js", {
			method: "HEAD",
			headers: { "If-Modified-Since": loaded },
		});
		if (res.status === 200) return location.reload();
		setTimeout(check, 1000);
	}
	check();
}

let map;
let canvasRenderer;
let svgRenderer;
let data;
let markers = {};
let visibleMarkers = [];
let selectedStation = null;
let connectedStations = new Map();
let hoverStation = null;
let hoverTime = 0;
let zoom = 6;

async function init() {
	const loading = document.getElementById("loading");

	try {
		const response = await fetch("data.json");
		data = await response.json();
		loading.classList.add("hidden");

		initMap();
		initMarkers();
		initSearch();
		loadFromHash();

		window.addEventListener("hashchange", loadFromHash);
	} catch (error) {
		loading.textContent = "Error loading data";
		console.error(error);
	}
}

function loadFromHash() {
	const hash = decodeURIComponent(window.location.hash.slice(1));
	if (hash) {
		const index = data.names.findIndex((name) => name === hash);
		if (index !== -1 && index !== selectedStation) {
			selectStation(index, true, false);
		}
	}
}

function updateHash() {
	if (selectedStation !== null) {
		const newHash = encodeURIComponent(data.names[selectedStation]);
		if (window.location.hash.slice(1) !== newHash) {
			window.location.hash = newHash;
		}
	} else {
		history.replaceState(null, "", window.location.pathname);
	}
}

function initMap() {
	map = L.map("map").setView([51.1657, 10.4515], zoom);
	canvasRenderer = L.canvas();
	svgRenderer = L.svg();

	const attribution = [
		'Map data: &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
		'Train data: <a href="https://gtfs.de/">GTFS.de</a> <a href="https://creativecommons.org/licenses/by/4.0/">CC BY 4.0</a>',
		data.version,
	].join(" | ");

	L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
		attribution,
	}).addTo(map);

	map.on("click", () => {
		clearSelection();
	});

	map.on("zoomend", () => {
		zoom = map.getZoom();
		updateMarkers();
	});
}

function initMarkers() {
	for (let i = 0; i < data.names.length; i++) {
		markers[i] = createMarker(i);
	}
}

function createMarker(i, visible, time) {
	const marker = L.circleMarker(data.coords[i], {
		interactive: true,
		renderer: visible ? svgRenderer : canvasRenderer,
	});
	marker.i = i;
	marker.visible = visible;
	updateMarkerStyle(marker);

	const tooltip =
		visible && connectedStations.has(i)
			? `${data.names[i]}: ${formatTime(time)}`
			: data.names[i];
	marker.bindTooltip(tooltip, {
		permanent: false,
		direction: "top",
		offset: [0, -8],
	});

	marker.on("click", onMarkerClick, marker);
	marker.on("mouseover", onMarkerMouseOver, marker);
	marker.on("mouseout", onMarkerMouseOut, marker);

	marker.addTo(map);

	return marker;
}

function onMarkerClick(e) {
	const marker = this;
	const i = marker.i;
	L.DomEvent.stopPropagation(e);
	marker.openTooltip();

	if (selectedStation === i) {
		return;
	}

	if (
		connectedStations.has(i) &&
		hoverStation === i &&
		Date.now() - hoverTime < 20
	) {
		// We're probably on mobile, don't select connected station on first click.
		return;
	}

	selectStation(i, false, false);
}

function onMarkerMouseOver() {
	const marker = this;
	const i = marker.i;
	hoverStation = i;
	hoverTime = Date.now();
	updateMarkerStyle(marker);
	marker.openTooltip();
}

function onMarkerMouseOut() {
	const marker = this;
	const i = marker.i;
	hoverStation = null;
	marker.closeTooltip();
	updateMarkerStyle(marker);
}

function updateMarkers() {
	Object.values(markers).forEach(updateMarkerStyle);
	visibleMarkers.forEach(updateMarkerStyle);
}

function getTimeColor(minutes) {
	const hue = 90 - Math.pow(minutes / 360, 0.6) * 90;
	return `hsl(${Math.max(0, hue)}, 70%, 50%)`;
}

function updateMarkerStyle(marker) {
	const { i, visible } = marker;
	const hover = hoverStation === i;
	const selected = visible && selectedStation === i;
	const connected = visible && connectedStations.has(i);
	const opacity = Number(visible || hover);
	const radius = Math.max(
		visible ? 2.5 : 0.5,
		Math.min(16, zoom) - (visible ? 6 : 8),
	);
	const weight = visible && !hover ? 1 : 3;
	const fillOpacity = visible ? 1 : Math.max(0, Math.min(1, zoom / 4 - 1));
	marker.setStyle({
		radius: radius + opacity * (weight / 2),
		fillColor: selected
			? "#0078A8"
			: connected
				? getTimeColor(connectedStations.get(i))
				: "#333",
		fillOpacity,
		color: hover ? "#fff" : "#333",
		opacity,
		weight,
	});
}

function initSearch() {
	const input = document.getElementById("search-input");
	const results = document.getElementById("search-results");
	let highlightedIndex = -1;
	let currentMatches = [];

	function updateResults(query) {
		if (query.length < 2) {
			results.classList.remove("active");
			currentMatches = [];
			highlightedIndex = -1;
			return;
		}

		currentMatches = [];
		const lowerQuery = query.toLowerCase();
		for (let i = 0; i < data.names.length; i++) {
			if (data.names[i].toLowerCase().includes(lowerQuery)) {
				currentMatches.push(i);
				if (currentMatches.length >= 10) break;
			}
		}

		results.innerHTML = currentMatches
			.map(
				(i) =>
					`<div class="search-result" data-index="${i}" tabindex="0">${data.names[i]}</div>`,
			)
			.join("");

		highlightedIndex = -1;
		results.classList.add("active");
	}

	function highlightNext() {
		if (currentMatches.length === 0) return;
		clearHighlight();
		highlightedIndex = (highlightedIndex + 1) % currentMatches.length;
		setHighlight();
	}

	function highlightPrev() {
		if (currentMatches.length === 0) return;
		clearHighlight();
		highlightedIndex =
			highlightedIndex <= 0 ? currentMatches.length - 1 : highlightedIndex - 1;
		setHighlight();
	}

	function clearHighlight() {
		const highlighted = results.querySelector(".highlighted");
		if (highlighted) highlighted.classList.remove("highlighted");
	}

	function setHighlight() {
		const items = results.querySelectorAll(".search-result");
		if (items[highlightedIndex]) {
			items[highlightedIndex].classList.add("highlighted");
			items[highlightedIndex].scrollIntoView({ block: "nearest" });
		}
	}

	function selectHighlighted() {
		if (
			highlightedIndex >= 0 &&
			currentMatches[highlightedIndex] !== undefined
		) {
			selectStation(currentMatches[highlightedIndex]);
			results.classList.remove("active");
			currentMatches = [];
			highlightedIndex = -1;
		}
	}

	input.addEventListener("input", () => updateResults(input.value));

	input.addEventListener("keydown", (e) => {
		if (!results.classList.contains("active")) return;

		switch (e.key) {
			case "ArrowDown":
				e.preventDefault();
				highlightNext();
				break;
			case "ArrowUp":
				e.preventDefault();
				highlightPrev();
				break;
			case "Enter":
				e.preventDefault();
				selectHighlighted();
				break;
			case "Escape":
				results.classList.remove("active");
				currentMatches = [];
				highlightedIndex = -1;
				break;
		}
	});

	results.addEventListener("click", (e) => {
		if (e.target.classList.contains("search-result")) {
			const index = parseInt(e.target.dataset.index);
			selectStation(index);
			results.classList.remove("active");
			currentMatches = [];
			highlightedIndex = -1;
		}
	});

	results.addEventListener("keydown", (e) => {
		if (e.target.classList.contains("search-result")) {
			switch (e.key) {
				case "ArrowDown":
					e.preventDefault();
					highlightNext();
					break;
				case "ArrowUp":
					e.preventDefault();
					highlightPrev();
					break;
				case "Enter":
					e.preventDefault();
					selectHighlighted();
					break;
			}
		}
	});

	document.addEventListener("click", (e) => {
		if (!e.target.closest("#search-container")) {
			results.classList.remove("active");
			currentMatches = [];
			highlightedIndex = -1;
		}
	});

	document.addEventListener("keydown", (e) => {
		if (e.key === "/" || (e.key === "k" && (e.ctrlKey || e.metaKey))) {
			e.preventDefault();
			input.focus();
			input.select();
		}
	});
}

function selectStation(index, center = true, animate = true) {
	clearSelection();
	selectedStation = index;

	const selectedMarker = createMarker(index, true);
	selectedMarker.openTooltip();
	visibleMarkers.push(selectedMarker);

	const edges = data.edges[index];
	const times = data.edgeTimes[index];

	for (let i = 0; i < edges.length; i++) {
		const destIdx = edges[i];
		const time = times[i];

		connectedStations.set(destIdx, time);

		const connectedMarker = createMarker(destIdx, true, time);
		visibleMarkers.push(connectedMarker);
	}

	if (center) {
		map.setView(data.coords[index], 9, { animate });
	}

	document.getElementById("search-input").value = data.names[index];

	updateHash();
}

function formatTime(minutes) {
	if (minutes < 60) return `${minutes} min`;
	const hours = Math.floor(minutes / 60);
	const mins = minutes % 60;
	return mins === 0 ? `${hours}h` : `${hours}h ${mins}m`;
}

function clearSelection() {
	if (selectedStation === null) return;

	selectedStation = null;
	connectedStations.clear();

	visibleMarkers.forEach((marker) => {
		marker.removeFrom(map);
	});
	visibleMarkers = [];

	document.getElementById("search-input").value = "";

	updateHash();
}

init();
