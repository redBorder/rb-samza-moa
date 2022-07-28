package net.redborder.samza.tasks;

import Exceptions.ConfigurationException;
import Exceptions.RBInstance;
import moa.cluster.Cluster;
import moa.cluster.Clustering;
import moa.cluster.SphereCluster;
import moa.clusterers.outliers.MCOD.MCOD;
import moa.clusterers.outliers.MyBaseOutlierDetector;
import moa.gui.visualization.DataPoint;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AnomalyDetectionTask implements StreamTask, InitableTask /*, WindowableTask */{
    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "rb_flow_post_moa");

    /**
     * Con este objeto podremos hacer loggin y seguir las acciones del programa
     */
    private static final Logger log = LoggerFactory.getLogger(AnomalyDetectionTask.class);

    /**
     * Con este objeto podremos asociar los flujos que recibe el detector con el conjunto de datos que utilizara
     * cada vez que se llegue al limite pasaremos a comprobarlo.
     */
    private Map<String, LinkedList<Instances>> instanciasFlujo = new HashMap<String, LinkedList<Instances>>();

    /**
     * Con este objeto podrmeos asociar a cada flujo de entrada su salida correspondiente
     */
    private Map<String, SystemStream> salidaSistema = new HashMap<String, SystemStream>();

    /**
     * Con este objeto podremos mapear los detectores a los que pertenece cada flujo, de manera que podremos enviar a cada
     * uno las instancias correspondientes a cada flujo con el objeto "instanciasFlujo"
     */
    private Map<String, Map<String, MCOD>> detectorFlujo = new HashMap<String, Map<String,MCOD>>();

    /**
     * Con este objeto podremos manipular la instancia actual del mensaje del flujo recibido, identificando previamente
     * este flujo.
     */
    private Instances datasetActualMOA = null;

    /**
     * Objeto que permite obtener el mapeo de los JSON
     */
    private ObjectMapper _mapper = new ObjectMapper();

    /**
     * Cadena que nos permitira identificar el flujo del que proviene el mensaje recibido
     * se va actualizando por cada mensaje que se recibe.
     */
    private String flujoRecibido = "<Flujo sin especificar>";
    /**
     * Con esta variable tendremos una marca temporal de la instancia recibida
     */
    private int timeStamp = 0;

    /**
     * Con esta variable tendremos el total de instancias procesadas
     */
    private int procesadas = 0;

    /**
     * Con este objeto tendremos disponible un buffer para el almacenamiento de los DataPoints
     */
    private LinkedList<DataPoint> buffer = new LinkedList<DataPoint>();

    /**
     * Con esta variable especificamos el límite de instancias procesadas para mostrar la información
     */
    private int totalProcesadas = 3000;

    /**
     * Con esta variable podemos comprobar si MOA ha sido configurado o no
     */
    private boolean configurado = false;

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {

        // Obtenemos el nombre del flujo
        flujoRecibido = envelope.getSystemStreamPartition().getStream();

        // Comprobamos si el mensaje que hemos recibido es de configuracion, de ser asi
        if(instanciasFlujo.containsKey(flujoRecibido)) {  // En caso contrario aplicamos el algoritmo al evento que se obtenga si se ha creado previamente



            // Con este objeto podremos obtener el JSON parseado y manipularlo de forma mas comoda
            // Creamos un mapeo de clave/valor para el evento que obtenemos del mensaje
            // Basicamente lo que estamos haciendo es coger nuestro JSON y hacerle un mapeado para que de esa manera
            // tengamos una acceso mas simple a los valores.
            Map<String, Object> evento = (Map<String, Object>)envelope.getMessage();

            // Obtenemos el conjunto de datasets asociados al flujo
            LinkedList<Instances> datasetsObtenidos = instanciasFlujo.get(flujoRecibido);

            // Obtenemos los detectores asociados al flujo
            Map<String, MCOD> detectoresAsociados = detectorFlujo.get(flujoRecibido);

            // Para cada dataset
            for(int i = 0; i < datasetsObtenidos.size(); i++){
                // Obtenemos del conjunto de datasets el dataset que actualmente usaremos
                datasetActualMOA = datasetsObtenidos.get(i);

                // Creamos la instancia
                RBInstance instancia = new RBInstance(datasetActualMOA.numAttributes());

                // Le asignamos el dataset
                instancia.setDataset(datasetActualMOA);

                // Escribimos el JSON que hemos obtenido como evento
                instancia.setEvent(_mapper.writeValueAsString(evento));

                // Comprobamos el número de atributos de clase que tiene la estrucutra de la instancia
                // Si tenemos un único atributo entonces obtenemos ese atributo

                if(instancia.numClasses() == 1){

                    // Obtenemos la única clase existente en la instancia
                    String clase = instancia.classAttribute().value(0);

                    if(evento.containsKey(clase)){
                        // Obtenemos el atributo
                        String atributo = evento.get(clase).toString();

                        if(atributo.matches("\\d{1,3}(\\.\\d{1,3}){3}")){

                            int[] octetos = ip2int(atributo);

                            for(int j = 0; j < instancia.numAttributes(); j++){

                                if(j < 4)
                                    instancia.setValue(j, octetos[j]);
                                else
                                    instancia.setValue(j, clase);
                            }

                        }else if(atributo.matches("\\d+")){
                            if(instancia.numValues() == 2) {
                                instancia.setValue(0, Double.valueOf(atributo));
                                instancia.setValue(1, clase);
                            }
                        }else if(atributo.matches(".+")){
                            if(instancia.numValues() == 2){
                                instancia.setValue(0, atributo);
                                instancia.setValue(1, clase);
                            }
                        }

                        // agregamos al dataset la nueva instancia
                        datasetActualMOA.add(instancia);
                        // Obtenemos el detector basandonos en el nombre de la relacion del dataset
                        MCOD detector = detectoresAsociados.get(datasetActualMOA.relationName());
                        // Entrenamos al detector con la instancia
                        detector.trainOnInstanceImpl(instancia);

                        // Una vez alcanzado el limite del dataset
                        if(datasetActualMOA.size() % totalProcesadas == 0) {

                            log.info("Information of dataset:\n\n" + datasetActualMOA.toSummaryString());

                            // Obtenemos las estadisticas
                            log.info(detector.getStatistics());

                            // Comprobamos si hemos detectado anomalías
                            if(!detector.getOutliersResult().isEmpty()){
                                // Para cada outlier
                                for(MyBaseOutlierDetector.Outlier o : detector.getOutliersResult()){
                                    // Obtenemos su instancia de referencia
                                    RBInstance instanciaAnomala = (RBInstance)o.inst;

                                    // Obtenemos el JSON del evento de la instancia
                                    Map<String, String> json = _mapper.readValue(instanciaAnomala.getEvent(), new TypeReference<HashMap<String, String>>(){});
                                    // Enriquecemos con el nuevo campo

                                    json.put("outlier",instanciaAnomala.classAttribute().value(0));

                                    //log.info("Detected outlier in event : \n" +_mapper.writeValueAsString(json));

                                    // Lo enviamos al sistema
                                    collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, json));

                                }
                            }

                            datasetActualMOA.clear();

                        }

                    }

                }

            }

        }

    }

    public int[] ip2int(String ipAddress){

        int[] octetos = {0, 0, 0, 0};

        String[] octets = ipAddress.split("\\.");

        int i = 3;

        for(String octet : octets){

            octetos[i] = Integer.parseInt(octet);
            i--;
        }

        return octetos;

    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(Config conf, TaskContext context) throws Exception {
        // Permitimos los campos que no estan delimitado por comillas dobles
        _mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        // Permitimos los campos que estan delimitados por comillas simples
        _mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);

        if(!configurado){

            if(conf.containsKey("outlier.review")) {
                totalProcesadas = Integer.parseInt(conf.get("outlier.review"));
                log.info("Set review at : " + totalProcesadas);
            }

            // Obtenemos el listado de flujos de entrada
            List<String> inputStreams = conf.getList("outlier.input");

            // Para cada flujo del listado
            // Comprobamos si tiene definida las clases
            for(String stream : inputStreams) {

                log.info("Stream configuration : " + stream);

                if (conf.containsKey("outlier." + stream + ".classes")) {

                    // Contador para conocer el número de datasets
                    int numDataset = 0;

                    // Obtenemos el listado de clases para el flujo definido
                    List<String> classes = conf.getList("outlier." + stream + ".classes");
                    // Lista enlazada de datasets
                    LinkedList<Instances> datasets = new LinkedList<Instances>();
                    // Mapeado de detectores
                    Map<String, MCOD> arrayDetectores = new HashMap<String, MCOD>();

                    // Tomaremos las propias clases como atributos de cadena, de forma que
                    // cada clase es en sí un atributo de cadena
                    for (String cls : classes) {
                        log.info("Added class : " + cls);

                        // Listado de atributos
                        ArrayList<Attribute> atributos = new ArrayList<Attribute>();

                        if (conf.containsKey("outlier." + stream + ".class." + cls)) {

                            List<String> attDefined = conf.getList("outlier." + stream + ".class." + cls);

                            for (String att : attDefined) {

                                Matcher m = Pattern.compile("(?<name>[_\\w][\\w\\d]*):(?<type>string|numeric|IP)").matcher(att);

                                if (m.matches()) {
                                    // Obtenemos el nombre del atributo
                                    String name_att = m.group("name");
                                    // Obtenemos el tipo del atributo
                                    String type_att = m.group("type");

                                    // Si es de tipo string
                                    if (type_att.equals("string")) {
                                        atributos.add(new Attribute(name_att, (ArrayList) null));
                                        log.info("Added attribute:<Type> : " + att);
                                        // Si es de tipo numérico
                                    } else if (type_att.equals("numeric")) {
                                        atributos.add(new Attribute(name_att));
                                        log.info("Added attribute:<Type> : " + att);
                                        // Si es de tipo "IP"
                                    } else if (type_att.equals("IP")) {

                                        for (int i = 1; i <= 4; i++)
                                            atributos.add(new Attribute("IP_segment_" + i));

                                        log.info("Added attribute:<Type> : " + att);
                                        // Si no es de ningún tipo anterior
                                    }else{
                                        log.error( m.group("type") + " is not a valid type! Attribute \"" + name_att + "\" not added!");
                                    }

                                }

                            }// fin bucle for de atributos

                        } else {
                            // Añadimos un nuevo atributo con el nombre del campo del flujo
                            atributos.add(new Attribute(cls, (ArrayList) null));
                            log.info("Added attribute:<Type> : " + cls + ":string");
                        }
                        // Clases en las que se clasificará el atributo
                        ArrayList<String> clasesNominales = new ArrayList<String>();
                        // Añadimos la clase a al grupo de clases nominales
                        clasesNominales.add(cls);

                        // Añadimos un nuevo atributo de clases con el nombre del campo del flujo
                        atributos.add(new Attribute("class", clasesNominales));

                        log.info("Added class attribute with content : " + clasesNominales.toString());

                        // Creamos un nuevo dataset
                        Instances dataset = new Instances(stream + "_dataset_" + numDataset++, atributos, 1000);
                        // Establecemos el índice del atributo de clases
                        dataset.setClassIndex(atributos.size() - 1);

                        log.info("Created dataset : \n\n" + dataset.toSummaryString());

                        // Agregamos el nuevo dataset
                        datasets.add(dataset);

                        // Creamos un nuevo detector
                        MCOD detector = new MCOD();

                        log.info("Created new outlier detector");

                        if (conf.containsKey("outlier." + stream + "." + cls + ".parameters")) {

                            log.info("Setting outlier detector parameters");
                            // Obtenemos los parámetros del detector
                            List<String> parametros = conf.getList("outlier." + stream + "." + cls + ".parameters");


                            for (String option : parametros) {

                                String[] opt = option.split(":");

                                if (opt[0].matches("window")) {
                                    detector.windowSizeOption.setValue(Integer.valueOf(opt[1]));
                                } else if (opt[0].matches("radius")) {
                                    detector.radiusOption.setValue(Double.valueOf(opt[1]));
                                } else if (opt[0].matches("k")) {
                                    detector.kOption.setValue(Integer.valueOf(opt[1]));
                                } else {
                                    log.error(opt[0] + " is not valid parameter!");
                                }
                            }
                        }
                        // Lo preparamos para el uso
                        detector.prepareForUse();

                        log.info("Outlier detector prepared with parameters: \n "
                                + "\twindow: " + detector.windowSizeOption.getValue() + "\n"
                                + "\tradius: " + detector.radiusOption.getValue() + "\n"
                                + "\tk: " + detector.kOption.getValue() + "");

                        log.info("Adding outlier detector with dataset : " + dataset.relationName());
                        // Agregamos el detector al dataset
                        arrayDetectores.put(dataset.relationName(), detector);

                    }

                    log.info("Adding datasets with stream : " + stream);
                    instanciasFlujo.put(stream, datasets);
                    log.info("Adding array of outlier detector with stream : " + stream);
                    detectorFlujo.put(stream, arrayDetectores);

                    log.info("Finish configuration of MOA for stream : \"" + stream + "\"");
                } else {
                    // En caso contrario lanzamos una excepción de configuración
                    throw new ConfigurationException("Don't defined outlier." + stream + ".classes");
                }
            }

            configurado = true;

        }

    }

    /**
     * Metodo que permite monitorizar las instancias de forma individual
     * @param instancia Instancia a inspeccionar de forma individual
     */
    public void inspeccionarConDetectorIndividualmente(Instance instancia) {

        // Obtenemos el array de detectores
        Map<String, MCOD> arrayDetectores = detectorFlujo.get(flujoRecibido);

        // COmprobamos si tenemos uno o mas detectores
        if (arrayDetectores.size() >= 1) {

            // Obtenemos el detector actual para dicha clase
            MCOD detectorActual = arrayDetectores.get(instancia.dataset());
            // Entrenamos el detector
            detectorActual.trainOnInstanceImpl(instancia);
            log.info(detectorActual.getStatistics());

            // Obtenemos el actual radio del detector
            double radioActualDetector = detectorActual.radiusOption.getValue();
            // Obtenemos la densidad actual del detector
            double densidadActualDetector = detectorActual.kOption.getValue();

            // Convertimos nuestra instancia
            RBInstance dInstancia = (RBInstance) instancia;

            // Asignamos un espacio temporal
            timeStamp++;
            // Obtenemos el numero de muestras procesadas
            procesadas++;

            // Creamos un nuevo punto para esa instancia en un tiempo fijo de tiempo
            DataPoint point = new DataPoint(instancia, timeStamp);
            // Almacenamos el punto en el buffer
            buffer.add(point);

            // Si superamos el limite de ventana
            if (buffer.size() > 1000) {
                // entonces eliminamos el primer elemento (que sera el mas antiguo)
                buffer.removeFirst();
            }

            // Creamos un nuevo cluster
            Clustering gtCluster = null;

            // Para cada punto de dato del buffer
            for (DataPoint dp : buffer) {
                // Actualizamos si peso basandonos en el espacio de tiempo asignado
                dp.updateWeight(timeStamp, 1000);
                // Y creamos un nuevo cluster con dicha informacion
                gtCluster = new Clustering(new ArrayList<DataPoint>(buffer));
            }

            log.info("Tamanio del cluster : " + gtCluster.size() + " cluster/s");

            // Variable para el procesado de los radios
            double radioAnterior = 0;

            // Para cada cluster que contiene
            for (Cluster c : gtCluster.getClustering()) {

                SphereCluster sc = (SphereCluster) c;

                log.info("[CLASE]RADIO GT : " + sc.getRadius());

                if (procesadas % 100 == 0)
                    if (radioAnterior == 0) {
                        radioAnterior = sc.getRadius();
//                    } else if (sc.getRadius() - radioAnterior <= EPSILON) {
//                        sumaRadios += sc.getRadius();
                    } else {
                        log.info("Se ha detectado un radio elevado, no se agregara a las estadisticas");
                    }

                log.info("[CLASE]DENSIDAD : " + sc.getWeight());

            }

//            }

        } else {
            log.info("No existen referencia a detectores para el flujo \"" + flujoRecibido + "\"");
        }
    }

    /**
     * este metodo permite normalizar el conjunto de datos, de esta forma podemos aplicar escalado con
     * valores comprendidos entre 0 y 1. Esto es necesario si la dimension de los valores son muy desproporcionadas.
     * @param dataset Conjunto de instancias que se va a normalizar
     */
    public void normalizarDataset(Instances dataset){

        // Creamos un minimo con el valor mas alto de un Double
        double min = Double.POSITIVE_INFINITY;
        // Creamos el maximo con el valor mas pequeño de un Double
        double max = Double.NEGATIVE_INFINITY;
        // Diferencia entre el maximo y el minimo sera 0.0
        double diffMaxMin = 0.0;

        for (int i = 0 ; i < dataset.numInstances(); i++){
            // Para cada instancia del dataset
            Instance instancia = dataset.get(i);
            for (int j = 0; j < instancia.numAttributes(); j++) {

                // Obtenemos el valor de sus atributos
                double value = instancia.value(j);
                // Obtenemos el minimo valor de todo el dataset
                if(value < min)
                    min = value;
                // Obtenemos el maximo valor de todo el dataset
                if(value > max)
                    max = value;
            }
        }

        // Una vez obtenido el maximo y el minimo efectuamos la diferencia de ambos
        diffMaxMin = max - min;

        for (int i = 0; i < dataset.numInstances(); i++){
            // Para cada instancia del dataset
            Instance instancia = dataset.get(i);

            for (int j = 0; j < instancia.numAttributes() ; j++) {
                // Comprobamos que la diferencia sea distinto de 1 y de 0
                if(diffMaxMin != 1 && diffMaxMin != 0){
                    // Obtenemos el valor de la instancia que hemos obtenido
                    double v = instancia.value(j);
                    // Hacemos la diferencia con el minimo valor que se ha obtenido y lo dividimos entre
                    // la diferencia del maximo y el minimo
                    v = (v - min)/diffMaxMin;
                    // Establecemos el nuevo valor para la instancia que hemos procesado
                    instancia.setValue(j, v);
                }
            }// For de atributos de instancia

        }// For de instancias

    }

/*
    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator)
            throws Exception {
    }
*/
}
