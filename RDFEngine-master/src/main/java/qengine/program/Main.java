package qengine.program;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.opencsv.CSVReader;
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
	static List<String> data = new ArrayList<String>();
	/**
	 * Votre répertoire de travail où vont se trouver les fichiers à lire
	 */
	static final String workingDir = "data/";

	/**
	 * Fichier contenant les requêtes sparql
	 */
	static final String queryFile = workingDir + "STAR_ALL_workload.queryset";

	/**
	 * Fichier contenant des données rdf
	 */
	static final String dataFile = workingDir + "100K.nt";

	static Map<Integer, List<Map<Integer, List<Integer>>>> posCopy = new HashMap<>();
	static Map<Integer, List<Map<Integer, List<Integer>>>> opsCopy = new HashMap<>();
	static Map<Integer, List<Map<Integer, List<Integer>>>> spoCopy = new HashMap<>();
	static Map<Integer, List<Map<Integer, List<Integer>>>> sopCopy = new HashMap<>();
	static Map<String, Integer> mapCopy = new HashMap<>();
	static int nbrequetes = 0;
	static long timeToParse = 0;
	static long timeToProcessAllQueries = 0;
	static int nbTriplets = 0;
	static long timeDico = 0;
	static long timeTriplet = 0;
	static long totalTime = 0;
	static int nbLignes = 0;
	static long totalWorkloadTime = 0;
	static int nbderequetesvides = 0;

	public static String getDataFile() {
		return dataFile;
	}

	public static void processAQuery(ParsedQuery query) throws IOException {
		List<StatementPattern> patterns = StatementPatternCollector.process(query.getTupleExpr());
		int[] firstTripletKey = new int[10];
		int[] secondTripletKey = new int[10];
		ArrayList<ArrayList<Integer>> mesListes = new ArrayList<ArrayList<Integer>>();
		ArrayList<Integer> resultatsFinal = new ArrayList<>();
		AtomicInteger in = new AtomicInteger();
		for (int i = 0; i < patterns.toArray().length; i++) {
			ArrayList<Integer> results = new ArrayList<>();
			in.set(i);
			if (patterns.get(i).getPredicateVar().getValue() == null) {
				if (mapCopy.get(patterns.get(i).getSubjectVar().getValue().toString()) != null) {
					firstTripletKey[i] = mapCopy.get(patterns.get(i).getObjectVar().getValue().toString());
				}
				if (mapCopy.get(patterns.get(i).getObjectVar().getValue().toString()) != null) {
					secondTripletKey[i] = mapCopy.get(patterns.get(i).getObjectVar().getValue().toString());
				}
				sopCopy.get(firstTripletKey[i]).forEach(m -> {
					if (m.get(secondTripletKey[in.get()]) != null) {
						Set<Integer> uniqueValues = new HashSet<>();
						uniqueValues.addAll(m.get(secondTripletKey[in.get()]));
						results.addAll(uniqueValues);
					}
				});
				mesListes.add(results);

			} else if (patterns.get(i).getObjectVar().getValue() == null) {
				if (mapCopy.get(patterns.get(i).getSubjectVar().getValue().toString()) != null) {
					firstTripletKey[i] = mapCopy.get(patterns.get(i).getPredicateVar().getValue().toString());
				}
				if (mapCopy.get(patterns.get(i).getObjectVar().getValue().toString()) != null) {
					secondTripletKey[i] = mapCopy.get(patterns.get(i).getObjectVar().getValue().toString());
				}
				spoCopy.get(firstTripletKey[i]).forEach(m -> {
					if (m.get(secondTripletKey[in.get()]) != null) {
						Set<Integer> uniqueValues = new HashSet<>();
						uniqueValues.addAll(m.get(secondTripletKey[in.get()]));
						results.addAll(uniqueValues);
					}
				});
				mesListes.add(results);
			} else {
				if (mapCopy.get(patterns.get(i).getPredicateVar().getValue().toString()) != null) {
					firstTripletKey[i] = mapCopy.get(patterns.get(i).getPredicateVar().getValue().toString());
				} else {
					nbderequetesvides++;
					return;
				}
				if (mapCopy.get(patterns.get(i).getObjectVar().getValue().toString()) != null) {
					secondTripletKey[i] = mapCopy.get(patterns.get(i).getObjectVar().getValue().toString());
				} else {
					nbderequetesvides++;
					return;
				}
				posCopy.get(firstTripletKey[i]).forEach(m -> {
					if (m.get(secondTripletKey[in.get()]) != null) {
						Set<Integer> uniqueValues = new HashSet<>();
						uniqueValues.addAll(m.get(secondTripletKey[in.get()]));
						results.addAll(uniqueValues);
					}
				});
				mesListes.add(results);
			}
		}
		resultatsFinal = calculIntersection(mesListes);

		if (resultatsFinal.size() == 0) {
			nbderequetesvides++;
		}
		exportResultsToCsv(resultatsFinal, "C:/Users/jerem/Documents/HAI914I/RDFEngine/RDFEngine-master/data/resultat_requetes.csv");
	}


	private static ArrayList<Integer> calculIntersection(ArrayList<ArrayList<Integer>> mesListes){
		ArrayList<Integer> results = new ArrayList<>();
		Set<Integer> uniqueValues = new HashSet<>();
		uniqueValues.addAll(mesListes.get(0));
		results.addAll(uniqueValues);

		for(int i = 1 ; i< mesListes.size(); i++) {
			if (results.size() == 0) {
				break;
			} else {
				List<Integer> list = mesListes.get(i);
				Set<Integer> result = list.stream()
						.distinct()
						.filter(results::contains)
						.collect(Collectors.toSet());
				results.clear();
				results.addAll(result);
			}
		}

		return results;
	}

	/**
	 * Entrée du programme
	 */
	public static <T, E> Set<T> getKeysByValue(Map<T, E> map, E value) {
		return map.entrySet()
				.stream()
				.filter(entry -> Objects.equals(entry.getValue(), value))
				.map(Map.Entry::getKey)
				.collect(Collectors.toSet());
	}


	public static void exportResultsToCsv(ArrayList<Integer> results,String path){

		FileWriter fw = null;
		try {
			fw = new FileWriter(path,true);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		ArrayList<String> values = new ArrayList<>();
		results.forEach(k-> {
					values.add(getKeysByValue(mapCopy,k).toString());
				}
		);
		try {
			for(String value : values){
				fw.write(","+value);
			}
			fw.write("\n");
			fw.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	public static void export(String path) throws Exception {
		FileWriter fw = null;
		try {
			fw = new FileWriter(path);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try {
			// Pour une meilleur lisibilité du csv on a choisie de mettre les informations
			// sur deux colonnes plutot que de les mettres sur 2 lignes
			fw.write("nom_fichier,");
			fw.write("nom_dossier_requetes,");
			fw.write("nb_triplets,");
			fw.write("nb_requetes,");
			fw.write("temps_lecture_data,");
			fw.write("temps_lecture_req,");
			fw.write("temps_creation_dico,");
			fw.write("nb_index,");
			fw.write("temps_creation_index,");
			fw.write("temps_exec_worload,");
			fw.write("temps_total\n");
			fw.write(getDataFile() + "," + getWorkingDir() + "," + nbLignes  + "," + nbrequetes + "," + timeToParse +"," + timeToProcessAllQueries
					+ "," + timeDico + "," + nbTriplets + "," + timeTriplet + ","  + totalWorkloadTime +"," +totalTime );
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	public static void main(String[] args) throws Exception {
		//BasicConfigurator.configure();
		long startWork = System.currentTimeMillis();
		long startTime = System.currentTimeMillis();
		for(int i = 0;i< args.length;i++){
			System.out.println(i + " " + args[i]);
		}

		String path1="C:/Users/jerem/Documents/500k.nt";
		parseData(args[3]);


		long endTime = System.currentTimeMillis();
		timeToParse = endTime - startTime;
		startTime = System.currentTimeMillis();
		String path2 = "C:/Users/jerem/Documents/HAI914I/RDFEngine/RDFEngine-master/data/STAR_ALL_workload.queryset";
		parseQueries(args[1]);
		endTime = System.currentTimeMillis();
		timeToProcessAllQueries = endTime - startTime;
		long endWork = System.currentTimeMillis();
		totalTime = endWork - startWork;
		String path = "C:/Users/jerem/Documents/HAI914I/RDFEngine/RDFEngine-master/data/resultat.csv";
		export(args[5]);
		System.out.print("");
		System.out.println("nb de requetes vides : " + nbderequetesvides);
	}

	private static String getWorkingDir() {
		return workingDir;
	}


	/**
	 * Traite chaque requête lue dans {@link #queryFile} avec {@link #processAQuery(ParsedQuery)}.
	 */
	private static void parseQueries(String path) throws FileNotFoundException, IOException {

		/**
		 * Try-with-resources
		 *
		 * @see <a href="https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html">Try-with-resources</a>
		 */
		/*
		 * On utilise un stream pour lire les lignes une par une, sans avoir à toutes les stocker
		 * entièrement dans une collection.
		 */
		try (Stream<String> lineStream = Files.lines(Paths.get(path))) {
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
					//temps d'evaluation d'une requete
					long start = System.currentTimeMillis();
					long stratTime = System.currentTimeMillis();
					processAQuery(query); // Traitement de la requête, à adapter/réécrire pour votre programme
					long endTime = System.currentTimeMillis();
					long timeTaken = (endTime-start);
					totalWorkloadTime += timeTaken;
					nbrequetes++;
					queryString.setLength(0); // Reset le buffer de la requête en chaine vide
				}
			}
		}
	}



	/**
	 * Traite chaque triple lu dans {@link #dataFile} avec {@link MainRDFHandler}.
	 */
	private static void parseData(String path) throws FileNotFoundException, IOException {
		long startTime = System.currentTimeMillis();
		MainRDFHandler mainRDFHandler =	new MainRDFHandler();
		try (Reader dataReader = new FileReader(path)) {
			// On va parser des données au format ntriples
			RDFParser rdfParser = Rio.createParser(RDFFormat.NTRIPLES);
			// On utilise notre implémentation de handler
			rdfParser.setRDFHandler(mainRDFHandler);
			// Parsing et traitement de chaque triple par le handler
			rdfParser.parse(dataReader, baseURI);

		}
//		System.out.println("dans le dico " + mapCopy.containsKey("http://db.uwaterloo.ca/~galuc/wsdbm/Country135"));
		mapCopy.putAll(mainRDFHandler.map);
		opsCopy.putAll(mainRDFHandler.ops);
		posCopy.putAll(mainRDFHandler.pos);
		spoCopy.putAll(mainRDFHandler.spo);
		sopCopy.putAll(mainRDFHandler.sop);
		nbTriplets = spoCopy.size();
		nbLignes = mainRDFHandler.nombreLigne;
		timeDico = mainRDFHandler.timeDico;
		timeTriplet = mainRDFHandler.timeTriplet;
	}
}
