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
	long timeDico = 0;
	long timeTriplet =0;
Map<String, Integer> map = new HashMap<>();
Map<Integer,List<Map<Integer, List<Integer>>>> sop = new HashMap<>();
Map<Integer,List<Map<Integer,List<Integer>>>> spo = new HashMap<>();
Map<Integer,List<Map<Integer, List<Integer>>>> pso = new HashMap<>();
Map<Integer,List<Map<Integer, List<Integer>>>> pos = new HashMap<>();
Map<Integer,List<Map<Integer, List<Integer>>>> ops = new HashMap<>();
Map<Integer,List<Map<Integer, List<Integer>>>> osp = new HashMap<>();

	@Override
	public void handleStatement(Statement st) {
		System.out.println("\n" + st.getSubject() + "\t " + st.getPredicate() + "\t " + st.getObject());
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
long endTime = System.currentTimeMillis();
timeDico +=endTime - startTime;

long startTimeTriplet = System.currentTimeMillis();
		//SOP
		if (!sop.containsKey(map.get(st.getSubject().stringValue()))) {
			List<Map<Integer, List<Integer>>> op = new ArrayList<>();
			sop.put(map.get(st.getSubject().stringValue()), op);

		}
		AtomicBoolean objectsop = new AtomicBoolean(false);
		if (sop.get(map.get(st.getSubject().stringValue())).size() > 0) {
			sop.get(map.get(st.getSubject().stringValue())).forEach(m -> {
				if (m.containsKey(map.get(st.getObject().stringValue()))) {
					m.get(map.get(st.getObject().stringValue())).add(map.get(st.getPredicate().stringValue()));
					objectsop.set(true);

				}

			});
		}
		if (!objectsop.get()) {

			HashMap<Integer, List<Integer>> op = new HashMap<>();
			List<Integer> list = new ArrayList<>();
			list.add(map.get(st.getPredicate().stringValue()));
			op.put(map.get(st.getObject().stringValue()), list);
			sop.get(map.get(st.getSubject().stringValue())).add(op);
		}


		if (!spo.containsKey(map.get(st.getSubject().stringValue()))) {
			//SPO
			List<Map<Integer, List<Integer>>> po = new ArrayList<>();
			spo.put(map.get(st.getSubject().stringValue()), po);
		}
		AtomicBoolean objectspo = new AtomicBoolean(false);
		if (spo.get(map.get(st.getSubject().stringValue())).size() > 0) {
			spo.get(map.get(st.getSubject().stringValue())).forEach(m -> {
				if (m.containsKey(map.get(st.getPredicate().stringValue()))) {
					m.get(map.get(st.getPredicate().stringValue())).add(map.get(st.getObject().stringValue()));
					objectspo.set(true);

				}

			});
		}
		if (!objectspo.get()) {
			HashMap<Integer, List<Integer>> po = new HashMap<>();
			List<Integer> list = new ArrayList<>();
			list.add(map.get(st.getObject().stringValue()));
			po.put(map.get(st.getPredicate().stringValue()), list);
			spo.get(map.get(st.getSubject().stringValue())).add(po);
		}



//PSO
		if(!pso.containsKey(map.get(st.getPredicate().stringValue())))
		{
			List<Map<Integer, List<Integer>>> so = new ArrayList<>();
			pso.put(map.get(st.getPredicate().stringValue()), so);

		}
		AtomicBoolean boolpso = new AtomicBoolean(false);
		if (pso.get(map.get(st.getPredicate().stringValue())).size() > 0) {
			pso.get(map.get(st.getPredicate().stringValue())).forEach(m -> {
				if (m.containsKey(map.get(st.getSubject().stringValue()))) {
					m.get(map.get(st.getSubject().stringValue())).add(map.get(st.getObject().stringValue()));
					boolpso.set(true);

				}
			});
		}
		if (!boolpso.get())
		{
			HashMap<Integer,List<Integer>> so = new HashMap<>();
			List<Integer> list = new ArrayList<>();
			list.add(map.get(st.getObject().stringValue()));
			so.put(map.get(st.getSubject().stringValue()),list);
			pso.get(map.get(st.getPredicate().stringValue())).add(so);

		}




						//POS
		if(!pos.containsKey(map.get(st.getPredicate().stringValue())))
		{
			List<Map<Integer, List<Integer>>> os = new ArrayList<>();
			pos.put(map.get(st.getPredicate().stringValue()), os);

		}
				AtomicBoolean boolpos = new AtomicBoolean(false);
				if (pos.get(map.get(st.getPredicate().stringValue())).size() > 0) {
					pos.get(map.get(st.getPredicate().stringValue())).forEach(m -> {
						if (m.containsKey(map.get(st.getObject().stringValue()))) {
							m.get(map.get(st.getObject().stringValue())).add(map.get(st.getSubject().stringValue()));
							boolpos.set(true);

						}
					});
				}
					if (!boolpos.get())
					{
						HashMap<Integer,List<Integer>> os = new HashMap<>();
						List<Integer> list = new ArrayList<>();
						list.add(map.get(st.getSubject().stringValue()));
						os.put(map.get(st.getObject().stringValue()),list);
						pos.get(map.get(st.getPredicate().stringValue())).add(os);

					}





        //OSP
		if(!osp.containsKey(map.get(st.getObject().stringValue())))
		{
			List<Map<Integer, List<Integer>>> sp = new ArrayList<>();
			osp.put(map.get(st.getObject().stringValue()), sp);
		}
					AtomicBoolean boolosp = new AtomicBoolean(false);
					if (osp.get(map.get(st.getObject().stringValue())).size() > 0) {
						osp.get(map.get(st.getObject().stringValue())).forEach(m -> {
							if (m.containsKey(map.get(st.getSubject().stringValue()))) {
								m.get(map.get(st.getSubject().stringValue())).add(map.get(st.getPredicate().stringValue()));
								boolosp.set(true);

							}
						});
					}
			if (!boolosp.get()){
				HashMap<Integer,List<Integer>> sp = new HashMap<>();
				List<Integer> list =new ArrayList<>();
				list.add(map.get(st.getPredicate().stringValue()));
				sp.put(map.get(st.getSubject().stringValue()),list);
				osp.get(map.get(st.getObject().stringValue())).add(sp);

			}



						//OPS
		if(!ops.containsKey(map.get(st.getObject().stringValue())))
		{
			List<Map<Integer, List<Integer>>> ps = new ArrayList<>();
			ops.put(map.get(st.getObject().stringValue()), ps);
		}


						AtomicBoolean boolops = new AtomicBoolean(false);
						if (ops.get(map.get(st.getObject().stringValue())).size() > 0) {
							ops.get(map.get(st.getObject().stringValue())).forEach(m -> {
								if (m.containsKey(map.get(st.getPredicate().stringValue()))) {
									m.get(map.get(st.getPredicate().stringValue())).add(map.get(st.getSubject().stringValue()));
									boolops.set(true);

								}
							});
						}
							if (!boolops.get())
							{
								HashMap<Integer,List<Integer>> ps = new HashMap<>();
								List<Integer> list = new ArrayList<>();
								list.add(map.get(st.getSubject().stringValue()));

								ps.put(map.get(st.getPredicate().stringValue()),list);
								ops.get(map.get(st.getObject().stringValue())).add(ps);

							}
							long endTimeTriplet = System.currentTimeMillis();
							timeTriplet += endTimeTriplet - startTimeTriplet;




	}
}