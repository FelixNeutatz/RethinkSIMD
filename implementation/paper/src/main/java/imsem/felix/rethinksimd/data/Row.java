package imsem.felix.rethinksimd.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by felix on 17.05.16.
 */
public class Row implements Serializable{
	public List<Value> row;
	
	public Row (List<Value> row) {
		this.row = row;
	}
	
	public Value get(int i) {
		return row.get(i);
	}

	@Override
	public String toString() {
		String s = "";
		for (int i = 0; i < row.size(); i++) {
			s += row.get(i) + ",";
		}
		s = s.substring(0,s.length() - 1);
		return s;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Row))
			return false;
		if (obj == this)
			return true;
		
		Row r = (Row) obj;
		
		for (int i = 0; i < row.size(); i++) {
			if ( !row.get(i).equals(r.get(i))) {
				return false;
			}
		}
		return true;
	}
}
