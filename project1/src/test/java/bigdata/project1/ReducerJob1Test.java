package bigdata.project1;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import bigdata.project1.Job1.ReducerJob1;
import bigdata.project1.Job1.WordOccurrences;

public class ReducerJob1Test {
	
	public static Iterable<WordOccurrences> getIterable(String...strings) {
		List<String> strs = Arrays.asList(strings);
		return strs.stream().map(s -> new WordOccurrences(s, 1)).collect(Collectors.toList());
	}
	
	private ReducerJob1 red;

	@Before
	public void setUp() throws Exception {
		red = new ReducerJob1();
	}

	@Test
	public void testGetSortedOccurrencesUnaParola() {
		Map<String, Integer> occ = this.red.getSortedOccurrences(getIterable("a", "a", "a"));
		
		assertEquals(3, occ.get("a").longValue());
	}
	
	
	@Test
	public void testGetSortedOccurrencesDueParola() {
		Map<String, Integer> occ = this.red.getSortedOccurrences(getIterable("a", "a", "a", "b", "b"));
		
		assertEquals(3L, occ.get("a").longValue());
		assertEquals(2L, occ.get("b").longValue());
	}
	
	
	@Test
	public void testGetSortedOccurrencesTreParola() {
		Map<String, Integer> occ = this.red.getSortedOccurrences(getIterable("a", "a", "a", "b", "b", "c"));
		
		assertEquals(3L, occ.get("a").longValue());
		assertEquals(2L, occ.get("b").longValue());
		assertEquals(1L, occ.get("c").longValue());
	}
	
	@Test
	public void testGetSortedOccurrencesUndiciParole() {
		Iterable<WordOccurrences> vals = getIterable("a", "a", "b", "b", "c", "c", "d", "d", "e", "e", "f", "f",
				"g", "g", "h", "h", "i", "i", "l", "l", "n");
		Map<String, Integer> occ = this.red.getSortedOccurrences(vals);
		
		assertEquals(10, occ.size());
		assertNull(occ.get("n"));
	}

}
