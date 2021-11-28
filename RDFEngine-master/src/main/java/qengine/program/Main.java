package qengine.program;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;

/**
 * Programme simple lisant un fichier de requête et un fichier de données.
 * 
 * <p>
 * Les entrées sont données ici de manière statique,
 * à vous de programmer les entrées par passage d'arguments en ligne de commande comme demandé dans l'énoncé.
 * </p>
 * 
 * <p>
 * Le présent programme se contente de vous montrer la voie pour lire les triples et requêtes
 * depuis les fichiers ; ce sera à vous d'adapter/réécrire le code pour finalement utiliser les requêtes et interroger les données.
 * On ne s'attend pas forcémment à ce que vous gardiez la même structure de code, vous pouvez tout réécrire.
 * </p>
 * 
 * @author Olivier Rodriguez <olivier.rodriguez1@umontpellier.fr>
 */
final class Main {
	static final String baseURI = null;

	/**
	 * Votre répertoire de travail où vont se trouver les fichiers à lire
	 */
	static final String workingDir = "data/";

	/**
	 * Fichier contenant les requêtes sparql
	 */
	static final String queryFile = workingDir + "sample_query.queryset";

	/**
	 * Fichier contenant des données rdf
	 */
	static final String dataFile = workingDir + "100K.nt";
	static  Map<Integer,List<Map<Integer, List<Integer>>>> posCopy = new HashMap<>();
	static Map<Integer,List<Map<Integer, List<Integer>>>> opsCopy = new HashMap<>();
	static Map<Integer,List<Map<Integer, List<Integer>>>> spoCopy = new HashMap<>();
	static Map<Integer,List<Map<Integer, List<Integer>>>> sopCopy = new HashMap<>();
	static Map<String, Integer> mapCopy = new HashMap<>();

	// ========================================================================

	/**
	 * Méthode utilisée ici lors du parsing de requête sparql pour agir sur l'objet obtenu.
	 */
	public static void processAQuery(ParsedQuery query ) {

		final Set<Integer> setToReturn = new HashSet<>();
		final Set<Integer> set1 = new HashSet<>();
		List<StatementPattern> patterns = StatementPatternCollector.process(query.getTupleExpr());
		int [] firstTripletKey = new int[10];
		int [] secondTripletKey= new int[10];
		List<Integer> results = new ArrayList<>();
		List<Integer> list1 = new ArrayList<>();
		List<Integer> list2 = new ArrayList<>();
		List<Integer> list3 = new ArrayList<>();


		for(int i =0;i<patterns.toArray().length;i++)
		{
			AtomicInteger in = new AtomicInteger();
			in.set(i);

			if(patterns.get(i).getPredicateVar().getValue()==null) {
				firstTripletKey[i] = mapCopy.get(patterns.get(i).getPredicateVar().getValue().toString());
				secondTripletKey[i] = mapCopy.get(patterns.get(i).getObjectVar().getValue().toString());
				computeQueryResult(patterns, firstTripletKey[i], secondTripletKey[i], results, list1, list2, list3, i, in, sopCopy);
			}
			else if(patterns.get(i).getObjectVar().getValue()==null){
				firstTripletKey[i] = mapCopy.get(patterns.get(i).getPredicateVar().getValue().toString());
				secondTripletKey[i] = mapCopy.get(patterns.get(i).getObjectVar().getValue().toString());
				computeQueryResult(patterns, firstTripletKey[i], secondTripletKey[i], results, list1, list2, list3, i, in, spoCopy);
			}
			else {
				firstTripletKey[i] = mapCopy.get(patterns.get(i).getPredicateVar().getValue().toString());
				secondTripletKey[i] = mapCopy.get(patterns.get(i).getObjectVar().getValue().toString());
				computeQueryResult(patterns, firstTripletKey[i], secondTripletKey[i], results, list1, list2, list3, i, in, posCopy);
			}
		}

		for (Integer yourInt : results)
		{
			if (!set1.add(yourInt))
			{
				setToReturn.add(yourInt);
			}
		}
		System.out.println("la liste " + setToReturn.size());

		setToReturn.forEach(System.out::println);

		System.out.println("variables to project : ");

		// Utilisation d'une classe anonyme
		query.getTupleExpr().visit(new AbstractQueryModelVisitor<RuntimeException>() {

			public void meet(Projection projection) {
				System.out.println(projection.getProjectionElemList().getElements());
			}
		});
	}

	private static void computeQueryResult(List<StatementPattern> patterns, int firstKey, int secondKey, List<Integer> results, List<Integer> list1, List<Integer> list2, List<Integer> list3, int i, AtomicInteger in, Map<Integer, List<Map<Integer, List<Integer>>>> triplet) {
		triplet.get(firstKey).forEach(m -> {
			if (m.get(secondKey) != null) {
				if (patterns.toArray().length > 1) {
					if (in.get() == 0) {
						list1.addAll(m.get(secondKey));
					} else {
						if (patterns.toArray().length < 3)
							results.addAll(m.get(secondKey));
					}

				}
				if (patterns.toArray().length > 2) {
					if (in.get() == 1) {
						list2.addAll(m.get(secondKey));
					} else {
						if (in.get() != 0 && patterns.toArray().length < 4) {

							results.addAll(m.get(secondKey).stream().distinct().collect(Collectors.toList()));
						}
					}

				}
				if (patterns.toArray().length > 3) {
					if (in.get() == 2) {
						list3.addAll(m.get(secondKey));
					} else {
						if (in.get() == 3) {
							results.addAll(m.get(secondKey).stream().distinct().collect(Collectors.toList()));
						}
					}
				}

				if (patterns.toArray().length == 1)
					results.addAll(m.get(secondKey));

			}

		});
		if (patterns.toArray().length > 1)
			if (i == 0) {
				List<Integer> list = list1.stream().distinct().collect(Collectors.toList());

				results.addAll(list);
			}
		if (patterns.toArray().length > 2)
			if (i == 1) {
				List<Integer> list = list2.stream().distinct().collect(Collectors.toList());

				results.addAll(list);
				final Set<Integer> setToReturn = new HashSet<>();
				final Set<Integer> set1 = new HashSet<>();

				for (Integer yourInt : results) {
					if (!set1.add(yourInt)) {
						setToReturn.add(yourInt);
					}
				}
				results.clear();
				results.addAll(setToReturn);
			}

		if (patterns.toArray().length > 3)
			if (i == 2) {
				List<Integer> list = list3.stream().distinct().collect(Collectors.toList());

				results.addAll(list);
				final Set<Integer> setToReturn = new HashSet<>();
				final Set<Integer> set1 = new HashSet<>();

				for (Integer yourInt : results) {
					if (!set1.add(yourInt)) {
						setToReturn.add(yourInt);
					}
				}
				results.clear();
				results.addAll(setToReturn);
				//results.forEach((k) -> System.out.println(k));
			}
	}


	/**
	 * Entrée du programme
	 */
	public static void main(String[] args) throws Exception {
		parseData();
		parseQueries();
	}

	// ========================================================================

	/**
	 * Traite chaque requête lue dans {@link #queryFile} avec {@link #processAQuery(ParsedQuery)}.
	 */
	private static void parseQueries() throws FileNotFoundException, IOException {
		/**
		 * Try-with-resources
		 * 
		 * @see <a href="https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html">Try-with-resources</a>
		 */
		/*
		 * On utilise un stream pour lire les lignes une par une, sans avoir à toutes les stocker
		 * entièrement dans une collection.
		 */
		try (Stream<String> lineStream = Files.lines(Paths.get(queryFile))) {
			SPARQLParser sparqlParser = new SPARQLParser();
			Iterator<String> lineIterator = lineStream.iterator();
			StringBuilder queryString = new StringBuilder();

			while (lineIterator.hasNext())
			/*
			 * On stocke plusieurs lignes jusqu'à ce que l'une d'entre elles se termine par un '}'
			 * On considère alors que c'est la fin d'une requête
			 */
			{
				String line = lineIterator.next();
				queryString.append(line);

				if (line.trim().endsWith("}")) {
					ParsedQuery query = sparqlParser.parseQuery(queryString.toString(), baseURI);
					processAQuery(query); // Traitement de la requête, à adapter/réécrire pour votre programme

					queryString.setLength(0); // Reset le buffer de la requête en chaine vide
				}
			}
		}
	}

	/**
	 * Traite chaque triple lu dans {@link #dataFile} avec {@link MainRDFHandler}.
	 */
	private static void parseData() throws FileNotFoundException, IOException {

		MainRDFHandler mainRDFHandler =	new MainRDFHandler();
		try (Reader dataReader = new FileReader(dataFile)) {
			// On va parser des données au format ntriples
			RDFParser rdfParser = Rio.createParser(RDFFormat.NTRIPLES);
			// On utilise notre implémentation de handler
			rdfParser.setRDFHandler(mainRDFHandler);
			// Parsing et traitement de chaque triple par le handler
			rdfParser.parse(dataReader, baseURI);

		}
/*
		mainRDFHandler.map.forEach((k,v)-> {
		System.out.println("cle " + k + " value " + v);
		});
*/

/*
		mainRDFHandler.pos.forEach((p,listos)->{
			listos.forEach(((l)->{
				l.forEach((o,listsub)->{
					listsub.forEach(s->{
						System.out.println("POS " +"<" +  p  + "," +o + "," + s+">");
					});
						});

			}));
		});
*/

mapCopy.putAll(mainRDFHandler.map);
opsCopy.putAll(mainRDFHandler.ops);
posCopy.putAll(mainRDFHandler.pos);
spoCopy.putAll(mainRDFHandler.spo);
sopCopy.putAll(mainRDFHandler.sop);


	}
}
