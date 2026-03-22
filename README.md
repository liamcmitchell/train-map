# Train Map

Interactive map of German train connections.

<img width="547" height="515" alt="image" src="https://github.com/user-attachments/assets/1c5b228e-7592-45cb-96c9-7d730b4dff41" />

Built with:

- [Python](https://python.org)
- [Leaflet.js](https://leafletjs.com)
- [OpenStreetMap](https://www.openstreetmap.org)
- [GTFS.de](https://gtfs.de)

## Development

```bash
python3 build.py --help
python3 build.py
python3 -m http.server 8000
```

## Architecture

- **build.py**: Downloads GTFS feeds, extracts connections, outputs compact `data.json`
- **app.js**: Vanilla JS frontend with Leaflet map
- **index.html**: Single page app with search autocomplete

## License

MIT

