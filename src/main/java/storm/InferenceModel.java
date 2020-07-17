package storm;

import org.tensorflow.SavedModelBundle;
import org.tensorflow.Session;

public class InferenceModel {
    private SavedModelBundle savedModelBundle;
    private Session session;

    public InferenceModel(String modelPath) {
        this.savedModelBundle = SavedModelBundle.load(modelPath, "serve");
        this.session = savedModelBundle.session();
    }

    public Session getSession() {
        return session;
    }
}
