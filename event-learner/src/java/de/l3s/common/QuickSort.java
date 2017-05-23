package de.l3s.common;

import java.util.ArrayList;
import java.util.List;

public class QuickSort {

	private static void quickSort(ArrayList list, int low0, int high0) {
		int low = low0, high = high0;

		if (low >= high) {
			return;
		} else if (low == high - 1) {
			if (((Comparable)list.get(low)).compareTo(list.get(high)) > 0) {
				Object temp = list.get(low);

				list.set(low, list.get(high));
				list.set(high, temp);
			}
			return;
		}
		Object pivot = list.get((low + high) / 2);

		list.set((low + high) / 2, list.get(high));
		list.set(high, pivot);

		while (low < high) {
			while (((Comparable)list.get(low)).compareTo(pivot) <= 0 && low < high) {
				low++;
			}

			while (((Comparable)list.get(high)).compareTo(pivot) >= 0 && low < high) {
				high--;
			}

			if (low < high) {
				Object temp = list.get(low);
				
				list.set(low, list.get(high));
				list.set(high, temp);
			}
		}

		list.set(high0, list.get(high));
		list.set(high, pivot);
		try {
		quickSort(list, low0, low - 1);
		quickSort(list, high + 1, high0);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void quickSort(List list, int low0, int high0) {
		int low = low0, high = high0;

		if (low >= high) {
			return;
		} else if (low == high - 1) {
			if (((Comparable)list.get(low)).compareTo(list.get(high)) > 0) {
				Object temp = list.get(low);

				list.set(low, list.get(high));
				list.set(high, temp);
			}
			return;
		}
		Object pivot = list.get((low + high) / 2);

		list.set((low + high) / 2, list.get(high));
		list.set(high, pivot);

		while (low < high) {
			while (((Comparable)list.get(low)).compareTo(pivot) <= 0 && low < high) {
				low++;
			}

			while (((Comparable)list.get(high)).compareTo(pivot) >= 0 && low < high) {
				high--;
			}

			if (low < high) {
				Object temp = list.get(low);
				
				list.set(low, list.get(high));
				list.set(high, temp);
			}
		}

		list.set(high0, list.get(high));
		list.set(high, pivot);
		try {
		quickSort(list, low0, low - 1);
		quickSort(list, high + 1, high0);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	public static void sort(ArrayList list) {
		quickSort(list, 0, list.size() - 1);
	}
	
	public static void sort(List list) {
		quickSort(list, 0, list.size() - 1);
	}
}
