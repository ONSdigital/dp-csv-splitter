FROM onsdigital/dp-go

WORKDIR /app/

COPY ./build/dp-csv-splitter .

ENTRYPOINT ./dp-csv-splitter
