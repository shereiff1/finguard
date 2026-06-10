import numpy as np
import onnxruntime as ort


class InferenceEngine:
    def __init__(self, model_path: str):
        opts = ort.SessionOptions()
        opts.intra_op_num_threads = 1
        opts.inter_op_num_threads = 1
        opts.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL

        self.session = ort.InferenceSession(model_path, sess_options=opts)
        self.input_name = self.session.get_inputs()[0].name

    def compute_fraud_score(self, features: list[float]) -> float:
        input_array = np.array([features], dtype=np.float32)
        outputs = self.session.run(None, {self.input_name: input_array})
        fraud_prob = float(outputs[1][0][1])
        return fraud_prob
