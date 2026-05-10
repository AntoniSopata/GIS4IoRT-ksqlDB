package com.gis;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKBReader;

@UdfDescription(name = "INGEOFENCE", description = "Checks if point is inside any of the provided polygons (separated by pipe)")
public class GeoUdf {

    private final GeometryFactory geometryFactory = new GeometryFactory();
    // Returns true if point is inside at least one polygon
    @Udf
    public Boolean INGEOFENCE(
        @UdfParameter(description = "lat") Double lat, 
        @UdfParameter(description = "lon") Double lon, 
        @UdfParameter(description = "hex_string") String hexString
    ) {
        if (lat == null || lon == null || hexString == null || hexString.isEmpty()) {
            return false;
        }

        try {
            Point point = geometryFactory.createPoint(new Coordinate(lon, lat));
            WKBReader reader = new WKBReader(geometryFactory);

            // Zone separation
            String[] hexes = hexString.split("\\|");

            for (String rawHex : hexes) {
                if (rawHex == null || rawHex.trim().isEmpty()) continue;
                
                try {
                    String cleanHex = rawHex.trim();

                    byte[] bytes = WKBReader.hexToBytes(cleanHex);
                    
                    Geometry polygon = reader.read(bytes);

                    if (polygon.contains(point)) {
                        return true; 
                    }
                } catch (Exception e) {
                    continue;
                }
            }

            return false;

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}