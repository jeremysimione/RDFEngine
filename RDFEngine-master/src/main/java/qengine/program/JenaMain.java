package qengine.program;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;

final class JenaMain {
    static final String baseURI = null;
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

    public static String getDataFile() {
        return dataFile;
    }

    public static void processAQuery(ParsedQuery query,Model model) throws IOException {
        ArrayList<String> resultatsFinal = new ArrayList<>();

        QueryExecution execution = QueryExecutionFactory.create(query.getSourceString(), model);
            StringBuilder st = new StringBuilder();
            try {
                ResultSet rs = execution.execSelect();
                List<QuerySolution> solution = ResultSetFormatter.toList(rs);
                if(solution.isEmpty()){ st.append("aucune Solution");nbderequetesvides++;}
                for (QuerySolution querySolution : solution) {
                    querySolution.varNames().forEachRemaining((varName) -> {
                        st.append(querySolution.get(varName));
                    });
                }
            } finally {
                execution.close();
            }
            resultatsFinal.add(st.toString());
            exportResultsToCsv(resultatsFinal,workingDir +"/resultat_requetes_jena.csv");


    }

    public static void exportResultsToCsv(ArrayList<String> results,String path){

        FileWriter fw = null;
        try {
            fw = new FileWriter(path,true);
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        try {
            for(String value : results){
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
        Model model = ModelFactory.createDefaultModel();
        model.read(dataFile);

        long startWork = System.currentTimeMillis();
        long startTime = System.currentTimeMillis();
        for(int i = 0;i< args.length;i++){
            System.out.println(i + " " + args[i]);
        }
        String path1=workingDir + "/100K.nt";


        long endTime = System.currentTimeMillis();
        timeToParse = endTime - startTime;
        startTime = System.currentTimeMillis();
        String path2 = workingDir +"/STAR_ALL_workload.queryset";
        parseQueries(path2,model);
        endTime = System.currentTimeMillis();
        timeToProcessAllQueries = endTime - startTime;
        long endWork = System.currentTimeMillis();
        totalTime = endWork - startWork;
        String path = workingDir + "resultat.csv";
        export(workingDir +"/resultat_jena.csv");
        System.out.print("");
        System.out.println("nb de requetes vides : " + nbderequetesvides);
        System.out.println("nb de requetes doublons : " + nbdeDoublons);
    }

    private static String getWorkingDir() {
        return workingDir;
    }


    /**
     * Traite chaque requête lue dans {@link #queryFile} avec {@link #processAQuery(ParsedQuery,Model model)}.
     */
    static void parseQueries(String path, Model model) throws FileNotFoundException, IOException {

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
                    processAQuery(query,model); // Traitement de la requête, à adapter/réécrire pour votre programme
                    long endTime = System.currentTimeMillis();
                    long timeTaken = (endTime-start);
                    totalWorkloadTime += timeTaken;
                    nbrequetes++;
                    queryString.setLength(0); // Reset le buffer de la requête en chaine vide
                }
            }
        }
    }
}
