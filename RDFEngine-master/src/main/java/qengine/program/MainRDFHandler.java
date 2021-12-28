package qengine.program;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Le RDFHandler intervient lors du parsing de données et permet d'appliquer un traitement pour chaque élément lu par le parseur.
 *
 * <p>
 * Ce qui servira surtout dans le programme est la méthode {@link #handleStatement(Statement)} qui va permettre de traiter chaque triple lu.
 * </p>
 * <p>
 * À adapter/réécrire selon vos traitements.
 * </p>
 */
public final class MainRDFHandler extends AbstractRDFHandler {
	int nombreLigne = 0;
	int num = 0;
	int numInv = 0;
	long timeDico = 0;
	long timeTriplet =0;
	Map<String, Integer> map = new HashMap<>();
	Map<Integer, String> mapInv = new HashMap<>();
	Map<Integer,Map<Integer, List<Integer>>> sop = new HashMap<>();
	Map<Integer,Map<Integer,List<Integer>>> spo = new HashMap<>();
	Map<Integer,Map<Integer, List<Integer>>> pso = new HashMap<>();
	Map<Integer,Map<Integer, List<Integer>>> pos = new HashMap<>();
	Map<Integer,Map<Integer, List<Integer>>> ops = new HashMap<>();
	Map<Integer,Map<Integer, List<Integer>>> osp = new HashMap<>();

	@Override
	public void handleStatement(Statement st) {
		//	System.out.println("\n" + st.getSubject() + "\t " + st.getPredicate() + "\t " + st.getObject());
		nombreLigne++;
		long startTime = System.currentTimeMillis();
		if (!map.containsKey(st.getSubject().stringValue())) {
			num++;
			map.put(st.getSubject().stringValue(), num);
		}
		if (!map.containsKey(st.getPredicate().stringValue())) {
			num++;
			map.put(st.getPredicate().stringValue(), num);
		}
		if (!map.containsKey(st.getObject().stringValue())) {
			num++;
			map.put(st.getObject().stringValue(), num);
		}

		//map inverse
		if (mapInv.get(st.getSubject().stringValue()) == null) {
			numInv++;
			mapInv.put(num,st.getSubject().stringValue());
		}
		if (mapInv.get(st.getPredicate().stringValue()) == null) {
			numInv++;
			mapInv.put( num,st.getPredicate().stringValue());
		}
		if (mapInv.get(st.getObject().stringValue()) == null) {
			numInv++;
			mapInv.put( num,st.getObject().stringValue());
		}


		long endTime = System.currentTimeMillis();
		timeDico +=endTime - startTime;

		long startTimeTriplet = System.currentTimeMillis();
		//SOP
		if (!sop.containsKey(map.get(st.getSubject().stringValue()))) {
			Map<Integer, List<Integer>> op = new HashMap<>();
			sop.put(map.get(st.getSubject().stringValue()), op);

		}
		AtomicBoolean objectsop = new AtomicBoolean(false);
		if (sop.get(map.get(st.getSubject().stringValue())).size() > 0) {
			if(!sop.get(map.get(st.getSubject().stringValue())).containsKey(map.get(st.getObject().stringValue()))){
				sop.get(map.get(st.getSubject().stringValue())).put(map.get(st.getObject().stringValue()), new ArrayList<>());
			}
			sop.get(map.get(st.getSubject().stringValue()))
					.get(map.get(st.getObject().stringValue()))
					.add(map.get(st.getPredicate().stringValue()));
			objectsop.set(true);


		}
		if (!objectsop.get()) {

			Map<Integer, List<Integer>> op = new HashMap<>();
			List<Integer> list = new ArrayList<>();
			list.add(map.get(st.getPredicate().stringValue()));
			op.put(map.get(st.getObject().stringValue()), list);
			sop.put(map.get(st.getSubject().stringValue()),op);
		}


		if (!spo.containsKey(map.get(st.getSubject().stringValue()))) {
			//SPO
			Map<Integer, List<Integer>> po = new HashMap<>();
			spo.put(map.get(st.getSubject().stringValue()), po);
		}
		AtomicBoolean objectspo = new AtomicBoolean(false);
		if (spo.get(map.get(st.getSubject().stringValue())).size() > 0) {
			if(!spo.get(map.get(st.getSubject().stringValue())).containsKey(map.get(st.getPredicate().stringValue()))){
				spo.get(map.get(st.getSubject().stringValue())).put(map.get(st.getPredicate().stringValue()), new ArrayList<>());
			}
			spo.get(map.get(st.getSubject().stringValue()))
					.get(map.get(st.getPredicate().stringValue()))
					.add(map.get(st.getObject().stringValue()));
			objectspo.set(true);


		}
		if (!objectspo.get()) {
			HashMap<Integer, List<Integer>> po = new HashMap<>();
			List<Integer> list = new ArrayList<>();
			list.add(map.get(st.getObject().stringValue()));
			po.put(map.get(st.getPredicate().stringValue()), list);
			spo.put(map.get(st.getSubject().stringValue()),po);
		}



//PSO
		if(!pso.containsKey(map.get(st.getPredicate().stringValue())))
		{
			Map<Integer, List<Integer>> so = new HashMap<>();
			pso.put(map.get(st.getPredicate().stringValue()), so);

		}
		AtomicBoolean boolpso = new AtomicBoolean(false);
		if (pso.get(map.get(st.getPredicate().stringValue())).size() > 0) {
			if(!pso.get(map.get(st.getPredicate().stringValue())).containsKey(map.get(st.getSubject().stringValue()))){
				pso.get(map.get(st.getPredicate().stringValue())).put(map.get(st.getSubject().stringValue()), new ArrayList<>());
			}
			pso.get(map.get(st.getPredicate().stringValue()))
					.get(map.get(st.getSubject().stringValue()))
					.add(map.get(st.getObject().stringValue()));

			boolpso.set(true);

		}


		if (!boolpso.get())
		{
			HashMap<Integer,List<Integer>> so = new HashMap<>();
			List<Integer> list = new ArrayList<>();
			list.add(map.get(st.getObject().stringValue()));
			so.put(map.get(st.getSubject().stringValue()),list);
			pso.put(map.get(st.getPredicate().stringValue()),so);

		}




		//POS
		if(!pos.containsKey(map.get(st.getPredicate().stringValue())))
		{
			Map<Integer, List<Integer>> os = new HashMap<>();
			pos.put(map.get(st.getPredicate().stringValue()), os);

		}
		AtomicBoolean boolpos = new AtomicBoolean(false);
		if (pos.get(map.get(st.getPredicate().stringValue())).size() > 0) {

			if(!pos.get(map.get(st.getPredicate().stringValue())).containsKey(map.get(st.getObject().stringValue()))){
				pos.get(map.get(st.getPredicate().stringValue())).put(map.get(st.getObject().stringValue()), new ArrayList<>());
			}
			pos.get(map.get(st.getPredicate().stringValue()))
					.get(map.get(st.getObject().stringValue()))
					.add(map.get(st.getSubject().stringValue()));
			boolpos.set(true);

		}
		if (!boolpos.get())
		{
			HashMap<Integer,List<Integer>> os = new HashMap<>();
			List<Integer> list = new ArrayList<>();
			list.add(map.get(st.getSubject().stringValue()));
			os.put(map.get(st.getObject().stringValue()),list);
			pos.put(map.get(st.getPredicate().stringValue()),os);

		}





		//OSP
		if(!osp.containsKey(map.get(st.getObject().stringValue())))
		{
			Map<Integer, List<Integer>> sp = new HashMap<>();
			osp.put(map.get(st.getObject().stringValue()), sp);
		}
		AtomicBoolean boolosp = new AtomicBoolean(false);
		if (osp.get(map.get(st.getObject().stringValue())).size() > 0) {
			if(!osp.get(map.get(st.getObject().stringValue())).containsKey(map.get(st.getSubject().stringValue()))){
				osp.get(map.get(st.getObject().stringValue())).put(map.get(st.getSubject().stringValue()), new ArrayList<>());
			}
			osp.get(map.get(st.getObject().stringValue()))
					.get(map.get(st.getSubject().stringValue()))
					.add(map.get(st.getPredicate().stringValue()));

			boolosp.set(true);


		}
		if (!boolosp.get()){
			HashMap<Integer,List<Integer>> sp = new HashMap<>();
			List<Integer> list =new ArrayList<>();
			list.add(map.get(st.getPredicate().stringValue()));
			sp.put(map.get(st.getSubject().stringValue()),list);
			osp.put(map.get(st.getObject().stringValue()),sp);

		}



		//OPS
		if(!ops.containsKey(map.get(st.getObject().stringValue())))
		{
			Map<Integer, List<Integer>> ps = new HashMap<>();
			ops.put(map.get(st.getObject().stringValue()), ps);
		}


		AtomicBoolean boolops = new AtomicBoolean(false);
		if (ops.get(map.get(st.getObject().stringValue())).size() > 0) {

			if(!ops.get(map.get(st.getObject().stringValue())).containsKey(map.get(st.getPredicate().stringValue()))){
				ops.get(map.get(st.getObject().stringValue())).put(map.get(st.getPredicate().stringValue()), new ArrayList<>());
			}
			ops.get(map.get(st.getObject().stringValue()))
					.get(map.get(st.getPredicate().stringValue()))
					.add(map.get(st.getSubject().stringValue()));
			boolops.set(true);

		}
		if (!boolops.get())
		{
			HashMap<Integer,List<Integer>> ps = new HashMap<>();
			List<Integer> list = new ArrayList<>();
			list.add(map.get(st.getSubject().stringValue()));

			ps.put(map.get(st.getPredicate().stringValue()),list);
			ops.put(map.get(st.getObject().stringValue()),ps);

		}
		long endTimeTriplet = System.currentTimeMillis();
		timeTriplet += endTimeTriplet - startTimeTriplet;




	}
}