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
			mapInv.put( num,st.getSubject().stringValue());

		}
		if (!map.containsKey(st.getPredicate().stringValue())) {
			num++;
			map.put(st.getPredicate().stringValue(), num);
			mapInv.put( num,st.getPredicate().stringValue());
		}
		if (!map.containsKey(st.getObject().stringValue())) {
			num++;
			map.put(st.getObject().stringValue(), num);
			mapInv.put(num,st.getObject().stringValue());
		}


		long endTime = System.currentTimeMillis();
		timeDico +=endTime - startTime;

		long startTimeTriplet = System.currentTimeMillis();
		//SOP
		if (!sop.containsKey(map.get(st.getSubject().stringValue()))) {
			Map<Integer, List<Integer>> op = new HashMap<>();
			sop.put(map.get(st.getSubject().stringValue()), op);

		}

			if(!sop.get(map.get(st.getSubject().stringValue())).containsKey(map.get(st.getObject().stringValue()))){
				sop.get(map.get(st.getSubject().stringValue())).put(map.get(st.getObject().stringValue()), new ArrayList<>());
			}

			sop.get(map.get(st.getSubject().stringValue()))
					.get(map.get(st.getObject().stringValue()))
					.add(map.get(st.getPredicate().stringValue()));




		if (!spo.containsKey(map.get(st.getSubject().stringValue()))) {
			//SPO
			Map<Integer, List<Integer>> po = new HashMap<>();
			spo.put(map.get(st.getSubject().stringValue()), po);
		}

			if(!spo.get(map.get(st.getSubject().stringValue())).containsKey(map.get(st.getPredicate().stringValue()))){
				spo.get(map.get(st.getSubject().stringValue())).put(map.get(st.getPredicate().stringValue()), new ArrayList<>());
			}
			spo.get(map.get(st.getSubject().stringValue()))
					.get(map.get(st.getPredicate().stringValue()))
					.add(map.get(st.getObject().stringValue()));


//PSO
		if(!pso.containsKey(map.get(st.getPredicate().stringValue())))
		{
			Map<Integer, List<Integer>> so = new HashMap<>();
			pso.put(map.get(st.getPredicate().stringValue()), so);

		}

			if(!pso.get(map.get(st.getPredicate().stringValue())).containsKey(map.get(st.getSubject().stringValue()))){
				pso.get(map.get(st.getPredicate().stringValue())).put(map.get(st.getSubject().stringValue()), new ArrayList<>());
			}
			pso.get(map.get(st.getPredicate().stringValue()))
					.get(map.get(st.getSubject().stringValue()))
					.add(map.get(st.getObject().stringValue()));



		//POS
		if(!pos.containsKey(map.get(st.getPredicate().stringValue())))
		{
			Map<Integer, List<Integer>> os = new HashMap<>();
			pos.put(map.get(st.getPredicate().stringValue()), os);

		}

			if(!pos.get(map.get(st.getPredicate().stringValue())).containsKey(map.get(st.getObject().stringValue()))){
				pos.get(map.get(st.getPredicate().stringValue())).put(map.get(st.getObject().stringValue()), new ArrayList<>());
			}
			pos.get(map.get(st.getPredicate().stringValue()))
					.get(map.get(st.getObject().stringValue()))
					.add(map.get(st.getSubject().stringValue()));




		//OSP
		if(!osp.containsKey(map.get(st.getObject().stringValue())))
		{
			Map<Integer, List<Integer>> sp = new HashMap<>();
			osp.put(map.get(st.getObject().stringValue()), sp);
		}

			if(!osp.get(map.get(st.getObject().stringValue())).containsKey(map.get(st.getSubject().stringValue()))){
				osp.get(map.get(st.getObject().stringValue())).put(map.get(st.getSubject().stringValue()), new ArrayList<>());
			}
			osp.get(map.get(st.getObject().stringValue()))
					.get(map.get(st.getSubject().stringValue()))
					.add(map.get(st.getPredicate().stringValue()));




		//OPS
		if(!ops.containsKey(map.get(st.getObject().stringValue())))
		{
			Map<Integer, List<Integer>> ps = new HashMap<>();
			ops.put(map.get(st.getObject().stringValue()), ps);
		}


			if(!ops.get(map.get(st.getObject().stringValue())).containsKey(map.get(st.getPredicate().stringValue()))){
				ops.get(map.get(st.getObject().stringValue())).put(map.get(st.getPredicate().stringValue()), new ArrayList<>());
			}
			ops.get(map.get(st.getObject().stringValue()))
					.get(map.get(st.getPredicate().stringValue()))
					.add(map.get(st.getSubject().stringValue()));

		long endTimeTriplet = System.currentTimeMillis();
		timeTriplet += endTimeTriplet - startTimeTriplet;



	}
}