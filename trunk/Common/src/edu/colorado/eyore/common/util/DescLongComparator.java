package edu.colorado.eyore.common.util;

import java.util.Comparator;

public class DescLongComparator implements Comparator<Long> {

	@Override
	public int compare(Long arg0, Long arg1) {
		if(arg0 != null && arg1 != null){
			return -1 * arg0.compareTo(arg1);
		}else if(arg0 == null){
			return 1;
		}else{
			return -1;
		}
			
	}

}
