package mygroup.myartifact;

import java.util.Iterator;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;

public class UnionJTS {
	private static GeometryFactory factory = new GeometryFactory();

	public static void main(String[] args) {
		Geometry[] polygons = new Geometry[5];
		polygons[0] = getRectangleFromLeftTopAndRightBottom(0.321534855, 0.036295831, -0.23567288, -0.415640992);
		polygons[1] = getRectangleFromLeftTopAndRightBottom(0.115064798, 0.105952147, -0.161920957, -0.405533972);
		polygons[2] = getRectangleFromLeftTopAndRightBottom(0.238709092, 0.016298271, -0.331934184, -0.18218141);
		polygons[3] = getRectangleFromLeftTopAndRightBottom(0.2069243, 0.223297076, -0.050542958, -0.475492946);
		polygons[4] = getRectangleFromLeftTopAndRightBottom(0.321534855, 0.036295831, -0.440428957, -0.289485599);

		Geometry res = polygons[0];
		for (int i = 1; i < polygons.length; i++) {
			res = res.union(polygons[i]);
		}

		System.out.println(res);
	}

	/**
	 * Get a rectangle from a input of 4 doubles, x1, y1, x2, y2, which are the left-top
	 * and right-bottom corner of a rectangle
	 */
	public static Geometry getRectangleFromLeftTopAndRightBottom(double x1, double y1, double x2, double y2) {
		Coordinate[] ps = new Coordinate[5];
		ps[0] = new Coordinate(x1, y1);
		ps[1] = new Coordinate(x2, y1);
		ps[2] = new Coordinate(x2, y2);
		ps[3] = new Coordinate(x1, y2);
		ps[4] = new Coordinate(x1, y1);
		LinearRing ring = factory.createLinearRing(ps);
		return factory.createPolygon(ring, null);
	}

	/**
	 * each string should be in format x1, y1, x2, y2, which are the left-top
	 * and right-bottom corner of a rectangle
	 */
	public static Geometry UnionRectangles(Iterator<String> it) {
		Geometry ret = null;
		try {
			while (null != it && it.hasNext()) {
				String str = it.next();
				String[] strs = str.split(",");
				if (null == strs || strs.length != 4)
					throw new Exception("Invalid input format:" + str);

				int size = strs.length;
				Double[] doubles = new Double[size];
				for (int i = 0; i < size; i++) {
					doubles[i] = Double.parseDouble(strs[i]);
				}

				Geometry next = getRectangleFromLeftTopAndRightBottom(doubles[0], doubles[1], doubles[2], doubles[3]);
				if (null == ret) {
					ret = next;
				} else {
					ret = ret.union(next);
				}
			}
		} catch (Exception e) {
			return null;
		}
		return ret;
	}

	/**
	 * Geometric Union a set of polygons
	 */
	public static Geometry UnionPolygons(Iterator<Geometry> it) {
		Geometry ret = factory.createPolygon(null, null);
		while (null != it && it.hasNext()) {
			ret = ret.union(it.next());
		}
		return ret;
	}
}