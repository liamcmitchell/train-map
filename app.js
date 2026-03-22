let map;
let data;
let markers = {};
let selectedStation = null;
let connectedStations = new Map();
let lastMouseover = { id: null, time: 0 };

function getTimeColor(minutes) {
    const hue = 90 - Math.pow(minutes / 360, 0.4) * 90;
    return `hsl(${Math.max(0, hue)}, 70%, 50%)`;
}

const DEFAULT_STYLE = {
    radius: 4,
    fillColor: '#999999',
    color: '#ffffff',
    weight: 2,
    fillOpacity: 1
};

const SELECTED_STYLE = {
    radius: 6,
    fillColor: '#0078A8',
    color: '#ffffff',
    weight: 2,
    fillOpacity: 1
};

const CONNECTED_STYLE = {
    radius: 6,
    color: '#ffffff',
    weight: 2,
    fillOpacity: 1
};

async function init() {
    const loading = document.getElementById('loading');
    
    try {
        const response = await fetch('data.json');
        data = await response.json();
        loading.classList.add('hidden');
        
        initMap();
        initMarkers();
        initSearch();
        loadFromHash();
        
        window.addEventListener('hashchange', loadFromHash);
    } catch (error) {
        loading.textContent = 'Error loading data';
        console.error(error);
    }
}

function loadFromHash() {
    const hash = decodeURIComponent(window.location.hash.slice(1));
    if (hash) {
        const index = data.names.findIndex(name => name === hash);
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
        history.replaceState(null, '', window.location.pathname);
    }
}

function initMap() {
    map = L.map('map', { preferCanvas: true }).setView([51.1657, 10.4515], 6);
    
    const attribution = [
        'Map data: &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
        'Train data: <a href="https://gtfs.de/">GTFS.de</a> <a href="https://creativecommons.org/licenses/by/4.0/">CC BY 4.0</a>',
        data.version
    ].join(' | ');
    
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution
    }).addTo(map);
    
    map.on('click', () => {
        clearSelection();
    });
}

function initMarkers() {
    const names = data.names;
    const coords = data.coords;
    
    for (let i = 0; i < names.length; i++) {
        const [lat, lon] = coords[i];
        
        const marker = L.circleMarker([lat, lon], DEFAULT_STYLE);
        marker.bindTooltip(names[i], { permanent: false, direction: 'top', offset: [0, -8] });
        markers[i] = marker;
        marker.addTo(map);
        
        const hitArea = L.circleMarker([lat, lon], {
            radius: 12,
            fillColor: 'transparent',
            fillOpacity: 0,
            stroke: false,
            interactive: true
        });
        
        hitArea.on('click', (e) => {
            L.DomEvent.stopPropagation(e);
            marker.openTooltip();

            if (selectedStation === i) {
                return;
            }

            if (connectedStations.has(i) && lastMouseover.id === i && Date.now() - lastMouseover.time < 20) {
                // We're probably on mobile, don't select connected station on first click.
                return;
            }
 
            selectStation(i, false, false);
        });
        
        hitArea.on('mouseover', () => {
            lastMouseover = { id: i, time: Date.now() };
            marker.setStyle({ weight: DEFAULT_STYLE.weight + 1 });
            marker.openTooltip();
        });
        
        hitArea.on('mouseout', () => {
            marker.closeTooltip();
            marker.setStyle({ weight: DEFAULT_STYLE.weight });
        });
        
        hitArea.addTo(map);
    }
}

function initSearch() {
    const input = document.getElementById('search-input');
    const results = document.getElementById('search-results');
    let highlightedIndex = -1;
    let currentMatches = [];
    
    function updateResults(query) {
        if (query.length < 2) {
            results.classList.remove('active');
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
        
        results.innerHTML = currentMatches.map(i => 
            `<div class="search-result" data-index="${i}" tabindex="0">${data.names[i]}</div>`
        ).join('');
        
        highlightedIndex = -1;
        results.classList.add('active');
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
        highlightedIndex = highlightedIndex <= 0 ? currentMatches.length - 1 : highlightedIndex - 1;
        setHighlight();
    }
    
    function clearHighlight() {
        const highlighted = results.querySelector('.highlighted');
        if (highlighted) highlighted.classList.remove('highlighted');
    }
    
    function setHighlight() {
        const items = results.querySelectorAll('.search-result');
        if (items[highlightedIndex]) {
            items[highlightedIndex].classList.add('highlighted');
            items[highlightedIndex].scrollIntoView({ block: 'nearest' });
        }
    }
    
    function selectHighlighted() {
        if (highlightedIndex >= 0 && currentMatches[highlightedIndex] !== undefined) {
            selectStation(currentMatches[highlightedIndex]);
            results.classList.remove('active');
            currentMatches = [];
            highlightedIndex = -1;
        }
    }
    
    input.addEventListener('input', () => updateResults(input.value));
    
    input.addEventListener('keydown', (e) => {
        if (!results.classList.contains('active')) return;
        
        switch (e.key) {
            case 'ArrowDown':
                e.preventDefault();
                highlightNext();
                break;
            case 'ArrowUp':
                e.preventDefault();
                highlightPrev();
                break;
            case 'Enter':
                e.preventDefault();
                selectHighlighted();
                break;
            case 'Escape':
                results.classList.remove('active');
                currentMatches = [];
                highlightedIndex = -1;
                break;
        }
    });
    
    results.addEventListener('click', (e) => {
        if (e.target.classList.contains('search-result')) {
            const index = parseInt(e.target.dataset.index);
            selectStation(index);
            results.classList.remove('active');
            currentMatches = [];
            highlightedIndex = -1;
        }
    });
    
    results.addEventListener('keydown', (e) => {
        if (e.target.classList.contains('search-result')) {
            switch (e.key) {
                case 'ArrowDown':
                    e.preventDefault();
                    highlightNext();
                    break;
                case 'ArrowUp':
                    e.preventDefault();
                    highlightPrev();
                    break;
                case 'Enter':
                    e.preventDefault();
                    selectHighlighted();
                    break;
            }
        }
    });
    
    document.addEventListener('click', (e) => {
        if (!e.target.closest('#search-container')) {
            results.classList.remove('active');
            currentMatches = [];
            highlightedIndex = -1;
        }
    });
    
    document.addEventListener('keydown', (e) => {
        if (e.key === '/' || (e.key === 'k' && (e.ctrlKey || e.metaKey))) {
            e.preventDefault();
            input.focus();
            input.select();
        }
    });
}

function selectStation(index, center = true, animate = true) {
    clearSelection();
    selectedStation = index;
    
    const [lat, lon] = data.coords[index];
    const name = data.names[index];
    
    markers[index].setStyle(SELECTED_STYLE);
    markers[index].openTooltip();
    
    if (center) {
        map.setView([lat, lon], 10, { animate });
    }
    
    document.getElementById('search-input').value = name;
    
    const edges = data.edges[index];
    const times = data.edgeTimes[index];
    
    for (let i = 0; i < edges.length; i++) {
        const destIdx = edges[i];
        const time = times[i];
        const color = getTimeColor(time);
        
        connectedStations.set(destIdx, time);
        
        markers[destIdx].setStyle({
            ...CONNECTED_STYLE,
            fillColor: color
        });

        markers[destIdx].setTooltipContent(`${data.names[destIdx]}: ${time} min`);
    }
    
    updateHash();
}

function clearSelection() {
    if (selectedStation === null) return;
    
    markers[selectedStation].setStyle(DEFAULT_STYLE);
    
    connectedStations.forEach((time, idx) => {
        markers[idx].setStyle(DEFAULT_STYLE);
        markers[idx].setTooltipContent(data.names[idx]);
        markers[idx].closeTooltip();
    });
    connectedStations.clear();
    
    selectedStation = null;
    document.getElementById('search-input').value = '';
    
    updateHash();
}

init();
