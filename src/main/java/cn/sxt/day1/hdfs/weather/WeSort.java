package cn.sxt.day1.hdfs.weather;

import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WeSort extends WritableComparator {

	public WeSort() {
		super(Weath.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Weath wa = (Weath) a;
		Weath wb = (Weath) b;
		return new CompareToBuilder()//.append(wb, wa)
				.append(wb.getYear(), wa.getYear()).append(wb.getMon(), wa.getMon())
				.append(wb.getWd(), wa.getWd()).toComparison();
	}
}
