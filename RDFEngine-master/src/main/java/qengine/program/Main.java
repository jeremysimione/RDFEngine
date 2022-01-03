package qengine.program;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;

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

	static Map<Integer, Map<Integer, List<Integer>>> posCopy = new HashMap<>();
	static Map<Integer, Map<Integer, List<Integer>>> opsCopy = new HashMap<>();
	static Map<Integer, Map<Integer, List<Integer>>> spoCopy = new HashMap<>();
	static Map<Integer, Map<Integer, List<Integer>>> sopCopy = new HashMap<>();
	static Map<String, Integer> mapCopy = new HashMap<>();
	static Map<Integer, String> mapCopyInv = new HashMap<>();
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
	static int nbdeDoublons = 0;
	static ArrayList<String> queries = new ArrayList<>();

	public static String getDataFile() {
		return dataFile;
	}

	public static void processAQuery(ParsedQuery query,String csvPath) {


		if(queries.contains(query.toString())) {
			nbdeDoublons++;
		}
		queries.add(query.toString());


		List<StatementPattern> patterns = StatementPatternCollector.process(query.getTupleExpr());
		int[] firstTripletKey = new int[10];
		int[] secondTripletKey = new int[10];
		ArrayList<ArrayList<Integer>> mesListes = new ArrayList<>();
		ArrayList<Integer> resultatsFinal;
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

				if (sopCopy.get(firstTripletKey[i]).get(secondTripletKey[in.get()]) != null) {
					Set<Integer> uniqueValues = new HashSet<>(sopCopy.get(firstTripletKey[i]).get(secondTripletKey[in.get()]));
					results.addAll(uniqueValues);
				}

				mesListes.add(results);

			} else if (patterns.get(i).getObjectVar().getValue() == null) {
				if (mapCopy.get(patterns.get(i).getSubjectVar().getValue().toString()) != null) {
					firstTripletKey[i] = mapCopy.get(patterns.get(i).getPredicateVar().getValue().toString());
				}
				if (mapCopy.get(patterns.get(i).getObjectVar().getValue().toString()) != null) {
					secondTripletKey[i] = mapCopy.get(patterns.get(i).getObjectVar().getValue().toString());
				}

				if (spoCopy.get(firstTripletKey[i]).get(secondTripletKey[in.get()]) != null) {
					Set<Integer> uniqueValues = new HashSet<>(spoCopy.get(firstTripletKey[i]).get(secondTripletKey[in.get()]));
					results.addAll(uniqueValues);
				}
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
				if (posCopy.get(firstTripletKey[i]).get(secondTripletKey[in.get()]) != null) {
					Set<Integer> uniqueValues = new HashSet<>(posCopy.get(firstTripletKey[i]).get(secondTripletKey[in.get()]));
					results.addAll(uniqueValues);
				}
				mesListes.add(results);
			}
		}

		resultatsFinal = mesListes.get(0);
		if(mesListes.size()>0) {
			for (int i = 1; i < mesListes.size(); i++) {
				resultatsFinal.retainAll(mesListes.get(i));
			}
		}
		if (resultatsFinal.size() == 0) {
			nbderequetesvides++;
		}
	exportResultsToCsv(resultatsFinal, csvPath.substring(0,csvPath.length()-4) +"_requetes.csv");
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
					values.add(mapCopyInv.get(k));
				}
		);
		try {
			if(values.size()==0){
				fw.write("aucune solution");
			}
			for(String value : values){
				fw.write(value +",");
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

		long startWork = System.currentTimeMillis();
		long startTime = System.currentTimeMillis();
		for(int i = 0;i< args.length;i++){
			System.out.println(i + " " + args[i]);
		}

		//String path1=workingDir + "/100K.nt";
		parseData(args[3]);
		//parseData(path1);

		long endTime = System.currentTimeMillis();
		timeToParse = endTime - startTime;
		startTime = System.currentTimeMillis();
		//String path2 = workingDir +"/STAR_ALL_workload.queryset";
		parseQueries(args[1],args[5]);
		//parseQueries(path2);

		endTime = System.currentTimeMillis();
		timeToProcessAllQueries = endTime - startTime;
		long endWork = System.currentTimeMillis();
		totalTime = endWork - startWork;
		//String path = workingDir + "/resultat.csv";
		export(args[5]);
		//export(path);
		System.out.print("");
		System.out.println("nb de requetes vides : " + nbderequetesvides);
		System.out.println("nb de requetes doublons : " + nbdeDoublons);
	}

	private static String getWorkingDir() {
		return workingDir;
	}


	/**
	 * Traite chaque requête lue dans {@link #queryFile} avec {@link #processAQuery(ParsedQuery,String)}.
	 */
	static void parseQueries(String path,String csvPath) throws FileNotFoundException, IOException {

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
					processAQuery(query,csvPath); // Traitement de la requête, à adapter/réécrire pour votre programme
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
		mapCopyInv.putAll(mainRDFHandler.mapInv);
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
