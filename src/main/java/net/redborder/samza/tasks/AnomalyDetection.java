package net.redborder.samza.tasks;

import Exceptions.ConfigurationException;
import Exceptions.RBInstance;
import moa.cluster.Cluster;
import moa.cluster.Clustering;
import moa.cluster.SphereCluster;
import moa.clusterers.outliers.MCOD.MCOD;
import moa.gui.visualization.DataPoint;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;

import java.util.*;

public class AnomalyDetection implements StreamTask, InitableTask /*, WindowableTask */{

    private final double EPSILON = 0.1;

    /**
     * Con este objeto podremos establecer un flujo hacia el topic de kafka "logMOA"
     */
    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "logMOA");

    /**
     * Con este objeto podremos hacer loggin y seguir las acciones del programa
     */
    private static final Logger log = LoggerFactory.getLogger(AnomalyDetection.class);

    /**
     * Con este objeto podremos asociar los flujos que recibe el detector con el conjunto de datos que utilizará
     * cada vez que se llegue al límite pasaremos a comprobarlo.
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
    private Instances datasetActual = null;

    /**
     * Objeto que permite obtener el mapeo de los JSON
     */
    private ObjectMapper _mapper = new ObjectMapper();

    /**
     * Cadena que nos permitirá identificar el flujo del que proviene el mensaje recibido
     * se va actualizando por cada mensaje que se recibe.
     */
    private String flujoRecibido = "<Flujo sin especificar>";

    /**
     * Objeto que nos permitirá tener una colección de series según las clases para ver la evolución
     * en la gráfica que la representa.
     */
//    XYSeriesCollection coleccionSeries = new XYSeriesCollection();

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

    // ================================================================================================= //
    //                                           CONSTANTES                                              //
    // Con las constantes podrmeos definir los distintos campos de la configuración de manera que será   //
    // fácil de editar en el caso que se quiera corregir un error o similar.                             //
    // ================================================================================================= //

    // Parámetro para especificar el flujo de entrada
    private final String PARAM_INPUT_STREAM = "input";
    // Parámetro para espcificar las  clases a monitorizar
    private final String PARAM_CLASSES = "classes";
    // Parámetro para especificar el tipo de detector de anomalías
    private final String PARAM_OUT_DET = "outlier_detection";
    // Parámetro para especificar el tipo de acción en la configuración
    private final String PARAM_ACTION = "action";
    // Parámetros para especificar el flujo de salida
    private final String PARAM_OUTPUT_STREAM = "output";

    /**
     * Método que permite aplicar una configuración a la actual tarea de Samza
     * @param configuracion Asociaciones por parejas clave:valor de cada uno de los parámetros
     * @throws ConfigurationException En el caso de incumplir con algún criterio
     */
    public void aplicarConfiguracion(Map<String, Object> configuracion) throws ConfigurationException {

        // ================================================================================================= //
        //                                           INPUT                                                   //
        // Se encargará de identificar el flujo de entrada que se quiere monitorizar, es necesario este paso //
        // si se desea que el detector inspeccione los atributos.                                            //
        // ================================================================================================= //

        log.info("Configuración que se aplicará : " + configuracion.toString());

        // Comprobamos si se ha especificado el flujo de entrada, en caso de no haberlo hecho se lanza una excepción
        if(!configuracion.containsKey(PARAM_INPUT_STREAM))
            throw  new ConfigurationException("Es obligatorio el uso del parámetro \"" + PARAM_INPUT_STREAM + "\" para definir el flujo objetivo de entrada");

        // Primero obtenemos el nombre del flujo que se va a tratar
        String flujoEntrada = configuracion.get(PARAM_INPUT_STREAM).toString();

        log.info("El flujo de entrada será : \"" + flujoEntrada + "\"");

        // ================================================================================================= //
        //                                           ACTION                                                  //
        // Básicamente indicará si lo que se desea es crear o eliminar la configuración, obsérvese que no    //
        // existe la posibilidad de modificación, dado que al modificar una configuración es necesario       //
        // destruir el detector, por ello siempre se puede crear y sustituir la configuración obteniendo el  //
        // mismo resultado. Al eliminar la configuración eliminamos en su totalidad                          //
        // ================================================================================================= //

        // Comprobamos si se ha especificado la acción de la configuración, en caso contrario lanzamos una excepcion
        if(!configuracion.containsKey(PARAM_ACTION))
            throw new ConfigurationException("Es obligatorio el uso del parámetro \"" + PARAM_ACTION + "\" para definir el tipo de configuración");

        // Por último obtenemos la acción de la aconfiguración
        String accion = configuracion.get("action").toString();

        // Comprobamos si la configuración contiene los parámetros adecuados
        if(!accion.matches("(create|delete)"))
            throw new ConfigurationException("El parámetro \"" + accion + "\" no es válido como opción para el atributo \"action\"\n" + configuracion.toString());

        // Si estamos creando una nueva configuración
        if(accion.equals("create")){

            // Aplicamos unos valores por defectos
            int windows = 1000;     // Ventana para 1000 instancias
            double radius = 0.1;    // Radio de 0.1
            int neightbours = 50;   // Número de vecinos de 50

            // ================================================================================================= //
            //                                           CLASSES                                                 //
            // Se encargará de identificar las clases en las que se agrupan los atributos, una instancia puede   //
            // tener de 1 a N clases. Por ejemplo, una instancia puede tener una clase "IP_X" si el atributo es  //
            // una IP con X siendo origen o destino en el caso de eventos de seguridad o puede tener una clase   //
            // "persona" siendo persona la unión de los atributos peso, altura y edad en el caso clínico.        //
            // ================================================================================================= //

            // Comprobamos si se ha especificado las clases que se van a monitorizar, en caso de no haberlo hecho se lanza una excepción
            if(!configuracion.containsKey(PARAM_CLASSES))
                throw new ConfigurationException("Es obligatorio el uso del parámetro \"" + PARAM_CLASSES + "\" para definir los tipos de instancias a monitorizar");

            // Segundo obtenemos las clases que sean para crear
            Map<String, Object> clases = (LinkedHashMap<String, Object>)configuracion.get(PARAM_CLASSES);

            log.info("Obtenida las siguientes clases con sus respectivos atributos y tipos : " + clases.toString());

            // ================================================================================================= //
            //                                           OD_TYPE                                                 //
            // Tipo de detector, aún no está aprobado, la idea es indicar si es un detector de instancia única   //
            // esto se da cuando los atributos comparten una distribución estadística similar y muy aproximada o //
            // si tendrá por el contrario un detector por cada tipo de atributo de manera que cada detector      //
            // inspecciona un atributo.                                                                          //
            // ================================================================================================= //

            // Por defecto el detector será distinto para cada clase
            String ODType = "dedicated";

            // Comprobamos si se ha especificado el tipo de detectores a utilizar
            if(configuracion.containsKey(PARAM_OUT_DET))
                // Tercero obtenemos el tipo de detector que se va a utilizar
            {
                // Obtenemos la configuración concreta del detector
                Map<String, Object> configDetector = (Map<String, Object>) configuracion.get(PARAM_OUT_DET);

                // Comprobamos si contiene el tipo del detector
                if(configDetector.containsKey("type"))
                    ODType = configDetector.get("type").toString();

                // Comprobamos si contiene los parámetros del detector
                if(configDetector.containsKey("parameters")) {
                    // Obtenemos los parámetros
                    Map<String, Object> parameters = (Map<String, Object>) configDetector.get("parameters");
                    // Asignamos el radio
                    radius = Double.valueOf(parameters.get("R").toString());
                    // Asignamos la ventana
                    windows = Integer.valueOf(parameters.get("W").toString());
                    // Asignamos el número de vecinos
                    neightbours = Integer.valueOf(parameters.get("K").toString());
                }
            }


            log.info("Se va a proceder a crear una nueva configuración para el flujo \"" + flujoEntrada + "\"");
            // Creamos un nuevo array de detectores al que se le asociará la clase que monitorizará, a un detector se le

            // asignará una clase en lugar de los atributos, ya que una clase puede estar compuesta por varios atributos
            Map<String, MCOD> arrayDetectores = new HashMap<String, MCOD>();

            // Creamos una lista enlzada de los datasets que tendrá el detector, por cada detector será necesario un dataset
            LinkedList<Instances> datasets = new LinkedList<Instances>();

            // Si el tipo de motor que necesitamos es único para cada clase entonces creamos un array de motores
            if(ODType.matches("dedicated")){

                log.info("Aplicaremos la configuración para varios detectores");

                // Para cada clase tendremos un detector en concreto
                log.info("Creación del conjunto de atributos para instancias de MOA de las clases seleccionadas: " + clases.keySet());

                // Con esta variable numeraremos los datasets
                int numeroDataset = 0;

                // Obtenemos todas las clases que hay definidas
                for(String s : clases.keySet()){

                    // -----------------------------------------------------------

                    // Creamos un array de atributos obteniendo para ello los dados por el JSON
                    ArrayList <Attribute> atributos = new ArrayList<Attribute>();

                    // Creamos un array con las clases que el detector va a tratar, definidas en el JSON
                    ArrayList<String> clasesDefinidas = new ArrayList<String>();

                    // El número de atributos será al inicio de 1
                    int maxNumeroAtributos = 1;

                    log.info("Se va a proceder a crear los atributos para la clase : \"" + s + "\"");

                    // Obtenemos los atributos para la clase que se ha seleccionado
                    Map<String, Object> atributosYTipos = (LinkedHashMap<String, Object>)clases.get(s);

                    // Comprobamos si el número de atributos es mayor al que se inicializó anteriormente de ser así se aplica
                    // dicho valor, de no ser así se deja el valor que tenía por defecto
                    maxNumeroAtributos = atributosYTipos.size() > maxNumeroAtributos ? atributosYTipos.size() : maxNumeroAtributos;

                    // Obtenemos los atributos y los tipos
                    for(Object a : atributosYTipos.keySet()){

                        log.info("Obtenido el atributo : " + a.toString());
                        /**
                         * Parte opcional que aún no está confirmada
                         */
                        if(!a.toString().equals("option")) {
                            if (atributosYTipos.get(a.toString()).toString().matches("N")) {
                                // Lo tratamos como un número ENTERO
                                atributos.add(new Attribute(a.toString()));
                            } else if (atributosYTipos.get(a.toString()).toString().matches("R")) {
                                // Lo tratamos como un número REAL
                                atributos.add(new Attribute(a.toString()));
                            } else {  // Lo tratamos como cadena
                                atributos.add(new Attribute(a.toString(), (ArrayList) null));
                            }
                        }else{
                            if(atributosYTipos.get(a.toString()).toString().equals("equals")){
                                log.info("Se va a proceder a aplicar los mismos atributos para la clase : " + s);
                            }
                        }

                    } // for de atributos

                    log.info("La clase \"" + s + "\" estará compuesta por " + maxNumeroAtributos + " atributos");

                    // Agreagamos la nueva clase

                    clasesDefinidas.add(s);
                    // Añadimos una nueva serie para monitorizar dicha clase

                    // Procuraremos siempre añadir la clase como el último elemento, así a la hora de crear la instancia
                    // podremos indicar que el índice en el que se encuentra las clases será la longitud del vector - 1
                    // esto es una ventaja a la hora de aumentar o disminuir el dataset.
                    atributos.add(new Attribute("class", clasesDefinidas));

                    log.info("Se ha creado el conjunto de clases : " + clasesDefinidas.toString());

                    // El conjunto de instancias tendrá como nombre el mismo que el del flujo, para poder identificarlo de
                    // una forma sencilla, contendrá los atributos definidos en el JSON y por defecto tendrán 1000 instancias
                    // de capacidad
                    Instances dataset = new Instances(flujoEntrada + "_dataset_" + numeroDataset++, atributos, 1000);
                    // Con el valor -1 indicamos que las clases para la clasificación es el último atributo agregado
                    dataset.setClassIndex(atributos.size() - 1);

//                    coleccionSeries.addSeries(new XYSeries(dataset.relationName()));

                    log.info("Se ha creado un nuevo dataset con las siguientes características : \n\n" + dataset.toSummaryString());

                    datasets.add(dataset);
                    // -----------------------------------------------------------

                    // Creamos un nuevo detector
                    MCOD detector = new MCOD();
                    detector.kOption.setValue(neightbours);
                    detector.windowSizeOption.setValue(windows);
                    detector.radiusOption.setValue(radius);
                    // Lo preparamos para su uso con los parámetros por defecto
                    detector.prepareForUse();

                    // Asociamos al detector la clase específica para él, esta clase contendrá los atributos necesarios
                    arrayDetectores.put(dataset.relationName(), detector);
                    log.info("Se ha creado y añadido un nuevo detector para la clase : \"" + s + "\"");

                }

                // Asociamos el conjunto de datasets al flujo a monitorizar
                instanciasFlujo.put(flujoEntrada, datasets);

                // Asociamos el array de detectores al flujo a monitorizar
                detectorFlujo.put(flujoEntrada, arrayDetectores);
                log.info("Se ha añadido un nuevo array de detectores asociado al flujo \"" + flujoEntrada + "\" de " + arrayDetectores.size() + " detector(es)");
                log.info("Se ha creado un conjunto de datasets de " + datasets.size() + " dataset(s)");

            }


            // ================================================================================================ //
            //                                           OUTPUT                                                 //
            // ================================================================================================ //

            // Obtenemos la salida de los resultados
            Map<String, String> salida = (Map<String,String>) configuracion.get(PARAM_OUTPUT_STREAM);

            // Obtenemos el sistema
            String sistema = salida.get("system");
            // Obtenemos su flujo de salida
            String flujoSalida = salida.get("stream");

            // Asociamos al flujo de entrada su correspondiente salida
            salidaSistema.put(flujoEntrada, new SystemStream(sistema, flujoSalida));

            log.info("Se ha añadido la salida del flujo al flujo \"" + flujoSalida + "\" del sistema \"" + sistema + "\" ");

        }else if(accion.equals("delete")){

            log.info("Se va a proceder a eliminar una configuración existente para el flujo \"" + flujoEntrada + "\"");
            // Eliminamos la instancia de flujo asociada
            instanciasFlujo.remove(flujoEntrada);
            // Eliminamos el detector del flujo asociado
            detectorFlujo.remove(flujoEntrada);

            salidaSistema.remove(flujoEntrada);

            log.info("Se ha eliminado el dataset, el flujo de salida y los detectores relacionados de forma satisfactoria");

        }
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {

        // Obtenemos el nombre del flujo
        flujoRecibido = envelope.getSystemStreamPartition().getStream();

        log.info("Se ha recibido un nuevo mensaje del flujo : " + flujoRecibido);

        // Comprobamos si el mensaje que hemos recibido es de configuración, de ser así
        if(flujoRecibido.equals("confMOA")){
            log.info("Se va a proceder a aplicar una nueva configuración");

            // Aplicamos la nueva configuración
            aplicarConfiguracion((Map<String, Object>) _mapper.readValue(envelope.getMessage().toString(), Map.class));
        }else if(instanciasFlujo.containsKey(flujoRecibido)) {  // En caso contrario aplicamos el algoritmo al evento que se obtenga si se ha creado previamente

            log.info("Se va a proceder a tratar los datos recibidos para el flujo \"" + flujoRecibido + "\"");

            // Con este objeto podremos obtener el JSON parseado y manipularlo de forma más cómoda
            // Creamos un mapeo de clave/valor para el evento que obtenemos del mensaje
            // Básicamente lo que estamos haciendo es coger nuestro JSON y hacerle un mapeado para que de esa manera
            // tengamos una acceso más simple a los valores.
            Map<String, Object> evento = (Map<String, Object>) _mapper.readValue(envelope.getMessage().toString(), Map.class);

            log.info("Recibido en el mensaje los valores : " + evento.toString());

            // Obtenemos el conjunto de datasets asociados al flujo
            LinkedList<Instances> datasetsObtenidos = instanciasFlujo.get(flujoRecibido);

            Map<String, MCOD> detectoresAsociados = detectorFlujo.get(flujoRecibido);

            // Para cada dataset
            for(int i = 0; i < datasetsObtenidos.size(); i++){
                // Obtenemos el actual
                datasetActual = datasetsObtenidos.get(i);

                // Creamos la instancia
                RBInstance instancia = new RBInstance(datasetActual.numAttributes());
                // Le asignamos el dataset
                instancia.setDataset(datasetActual);
                // Escribimos el JSON que hemos obtenido como evento
                instancia.setEvent(_mapper.writeValueAsString(evento));

                log.info("Se ha creado una nueva instancia para el flujo " + flujoRecibido + " con " + instancia.numAttributes() + " atributos");
                // De la instancia creada
                for (int j = 0; j < instancia.numAttributes(); j++) {
                    // Obtenemos el atributo
                    String atributo = instancia.attribute(j).name();
                    log.info("Se ha obtenido el atributo : " + atributo);

                    // Comprobamos el tipo de atributo, en el caso que sea numérico
                    if (instancia.attribute(j).isNumeric()) {
                        instancia.setValue(j, Double.valueOf(evento.get(atributo).toString()));
                        // En el caso de que el atributo sea una cadena
                    } else if (instancia.attribute(j).isString()) {
                        instancia.setValue(j, evento.get(atributo).toString());
                        // En caso de que no fuera nada de lo anterior
                    } else {

                        // Obtenemos el número de atributos
                        Enumeration<Object> atributos = instancia.classAttribute().enumerateValues();

                        // Mientras que haya atributos
                        while(atributos.hasMoreElements()) {
                            // Obtenemos la clase nominal
                            String claseNominal = atributos.nextElement().toString();
                            log.info("Valor de clase posible : " + claseNominal);

                            // Si la clase nominal coincide con el atributo entonces
                            if(claseNominal.equals(atributo))
                                // Obtenemos el valor del atributo del evento y lo asignamos a la instancia
                                instancia.setValue(j, evento.get(atributo).toString());
                            else {  // Si no coincide
                                log.info("No se ha encontrado una clave para el atributo : " + atributo);
                                // Añadimos a la instancia el valor de la clase nominal
                                instancia.setValue(j, claseNominal);
                            }
                        }

                    }
                }

                log.info("Se ha creado la instancia con valores [" + instancia.enumerateAttributes() + "]: " + instancia.toString());

                // Añadimos al dataset la nueva instancia
                datasetActual.add(instancia);
                log.info("Se ha agregado la instancia al dataset \"" + datasetActual.relationName() + "\" con éxito");

                log.info("El dataset actual \"" + datasetActual.relationName() + "\" tiene un tamaño de " + datasetActual.size());

                // Obtenemos el detector basándonos en el nombre de la relación del dataset
                MCOD detector = detectoresAsociados.get(datasetActual.relationName());

                log.info("DATASET OBTENIDO PARA PROCESADO : " + datasetActual.toSummaryString());

                // Entrenamos al detector con la instancia
                detector.trainOnInstanceImpl(instancia);

                // Una vez alcanzado el tamaño del dataset
                if(datasetActual.size() % 1000 == 0) {
                    log.info("Se han llegado a las 1000 instancias");
                    // Obtenemos todos los outliers detectados
                    for (int k = 0; k < detector.getOutliersResult().size(); k++) {
                        // Obtenemos la instancia anómala
                        RBInstance instanciaAnomala = (RBInstance)detector.getOutliersResult().get(k).inst;
                        log.info("Instancia detectada como anómala : " + instanciaAnomala.getEvent());
                        // Creamos el evento asociado a la instancia
                        Map<String, Object> eventoAnomalo = (Map<String, Object>)_mapper.readValue(instanciaAnomala.getEvent(), Map.class);
                        // Añadimos la nueva cadena donde ponemos el nuevo valor "outlier"
                        eventoAnomalo.put("outlier","[" + instanciaAnomala.toString() + "]");

                        // Reenviamos por el sistema que se ha escogido
                        collector.send(new OutgoingMessageEnvelope(salidaSistema.get(flujoRecibido), _mapper.writeValueAsString(eventoAnomalo)));

                    }
                }
                // Obtenemos las estadísticas
                log.info(detector.getStatistics());

            }

        }else{
            log.info("El flujo \"" + flujoRecibido + "\" no será tratado por no está especificado en la configuración");
        }

    }


    @SuppressWarnings("unchecked")
    @Override
    public void init(Config conf, TaskContext context) throws Exception {
        // Permitimos los campos que no están delimitado por comillas dobles
        _mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        // Permitimos los campos que están delimitados por comillas simples
        _mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    }

    /**
     * Método que permite monitorizar las instancias de forma individual
     * @param instancia Instancia a inspeccionar de forma individual
     */
    public void inspeccionarConDetectorIndividualmente(Instance instancia) {

        // Obtenemos el array de detectores
        Map<String, MCOD> arrayDetectores = detectorFlujo.get(flujoRecibido);

        // COmprobamos si tenemos uno o más detectores
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
            // Obtenemos el número de muestras procesadas
            procesadas++;

            // Creamos un nuevo punto para esa instancia en un tiempo fijo de tiempo
            DataPoint point = new DataPoint(instancia, timeStamp);
            // Almacenamos el punto en el buffer
            buffer.add(point);

            // Si superamos el tamaño de ventana
            if (buffer.size() > 1000) {
                // entonces eliminamos el primer elemento (que será el más antiguo)
                buffer.removeFirst();
            }

            // Creamos un nuevo cluster
            Clustering gtCluster = null;

            // Para cada punto de dato del búffer
            for (DataPoint dp : buffer) {
                // Actualizamos si peso basándonos en el espacio de tiempo asignado
                dp.updateWeight(timeStamp, 1000);
                // Y creamos un nuevo clúster con dicha información
                gtCluster = new Clustering(new ArrayList<DataPoint>(buffer));
            }

            log.info("Tamaño del clúster : " + gtCluster.size() + " clúster/s");

            // Variable para el procesado de los radios
            double radioAnterior = 0;

            // Para cada clúster que contiene
            for (Cluster c : gtCluster.getClustering()) {

                SphereCluster sc = (SphereCluster) c;

                log.info("[CLASE]RADIO GT : " + sc.getRadius());

                if (procesadas % 100 == 0)
                    if (radioAnterior == 0) {
                        radioAnterior = sc.getRadius();
                    } else if (sc.getRadius() - radioAnterior <= EPSILON) {
//                        sumaRadios += sc.getRadius();
                    } else {
                        log.info("Se ha detectado un radio elevado, no se añadirá a las estadísticas");
                    }

                log.info("[CLASE]DENSIDAD : " + sc.getWeight());

            }

//            }

        } else {
            log.info("No existen referencia a detectores para el flujo \"" + flujoRecibido + "\"");
        }
    }

    /**
     * Éste método permite normalizar el conjunto de datos, de esta forma podemos aplicar escalado con
     * valores comprendidos entre 0 y 1. Esto es necesario si la dimension de los valores son muy desproporcionadas.
     * @param dataset Conjunto de instancias que se van a normalizar
     */
    public void normalizarDataset(Instances dataset){

        // Creamos un mínimo con el valor más alto de un Double
        double min = Double.POSITIVE_INFINITY;
        // Creamos el máximo con el valor más pequeño de un Double
        double max = Double.NEGATIVE_INFINITY;
        // Diferencia entre el máximo y el mínimo será 0.0
        double diffMaxMin = 0.0;

        for (int i = 0 ; i < dataset.numInstances(); i++){
            // Para cada instancia del dataset
            Instance instancia = dataset.get(i);
            for (int j = 0; j < instancia.numAttributes(); j++) {

                // Obtenemos el valor de sus atributos
                double value = instancia.value(j);
                // Obtenemos el mínimo valor de todo el dataset
                if(value < min)
                    min = value;
                // Obtenemos el máximo valor de todo el dataset
                if(value > max)
                    max = value;
            }
        }

        // Una vez obtenido el máximo y el mínimo efectuamos la diferencia de ambos
        diffMaxMin = max - min;

        for (int i = 0; i < dataset.numInstances(); i++){
            // Para cada instancia del dataset
            Instance instancia = dataset.get(i);

            for (int j = 0; j < instancia.numAttributes() ; j++) {
                // Comprobamos que la diferencia sea distinto de 1 y de 0
                if(diffMaxMin != 1 && diffMaxMin != 0){
                    // Obtenemos el valor de la instancia que hemos obtenido
                    double v = instancia.value(j);
                    // Hacemos la diferencia con el mínimo valor que se ha obtenido y lo dividimos entre
                    // la diferencia del máximo y el mínimo
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
