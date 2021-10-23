# Prototyping Datanectar architecture

## Goals

* Clear code separation between Mad Consulting's shared ML Pipeline codebase and client code (e.g. "pip install datanectar")
	- Both for ease of updating and legal protection.
* Flexibility (not tying ourselves too tightly to components like Luigi or Feast).
	- Things are changing so quicly in the ML space, it is wise not to be too tightly-coupled.
* Simplicity (which may at times be inversely proportional to Flexibility)


## Design questions

### If we `pip install datanecdtar`, how do we start servers (luigi, feast, etc)?

#### Option 1: `datanectar.serve()`
	* In this option, we would allow configuration of subcomponents like luigi and feast through datanectar configs, which would allow `datanectar.serve()` to orchestrate all subcomponents
	* Pros:
		- Simplicity of use (1 call to start all servers instead of like 3).
	* Cons:
		- Documentation around subcomponents like luigi and feast becomes useless when configuring via datanectar, since datanectar configs would be used in place of original component configs.
		- Loss of power (flexibility) of underlying components (unless we fully implement datanectar abstractions for each subcomponent functionality).
#### Option 2: code generation (or examples which could be copied/pasted) which keep all subcomponents at the forefront.
	* In this option, we would probably have something like a `docker-compose.yml` file which would expose subcomponents directly (and facilitate in starting them).
	* Pros:
		- Flexibility and full power of all subcomponents (since we are not trying to abstract them)
	* Cons:
		- Loss of simplicity of use (relative to a simple `datanectar.serve()`).

---




### Datanectar code structure: subclasses? functions? decorators?

This question partially depends on the previous question. If we want to provide a `datanectar.serve()`-like interface, we would probably be pushed more towards the top-level `DataNectarTask` sort of class inheritance pattern.

#### Option 1: Class inheritance

This would look something like:


class BuildFeatures(datanectar.NectarTask)
    def __init__(self):
        pass

    def output(self):
        return self.get_artifact_path()

    def do_stuff(self):
        """Custom stuff"""
        return [1, 2, 3, 4]

    def run(self):
        """Custom stuff"""
        result = self.do_stuff()
        artifact_path = self.get_artifact_path()
        self.store_results(artifact_path, result)

#### Option 2: functional composition

In this option, we would expose luigi, feast, and any other components directly, and datanectar's artifact naming and caching system would be called into via functions.

Pseudocode example:

	import luigi
	import datanectar

	class FeatureExtractorTask(luigi.Task):
	    def __init__(self, task_type=None, model_type=None, storage_layer=None):
		self.task_type = task_type
		self.model_type = model_type
		self.storage_layer = storage_layer

	    def do_stuff(self):
		"""Custom code"""

	    def run(self):
		result = self.do_stuff()
		artifact_name = datanectar.get_artifact_name(self)
		self.storage_layer.store(artifact_name, result)


#### Option 3: function decorators

Example code:

	def do_stuff():
	    return result

	@datanectar.Task(task_type="train_model", model_family="classifier")
	def train_my_classifier_task():
	    # The decorator here would take the return value, and store it in the proper location.
	    return do_stuff()

* Problem: How would we transform this kind of function into a `luigi.Task` subclass? Perhaps class decorators would be a better option?

#### Option 4: class decorators

Perhaps a mix of classes

Example code:

        import luigi
        import datanectar

        @datanectar.Task(task_type='train_model', model_family='classifier', storage_layer='s3')
        class TrainETAClassifier(luigi.Task):
            def do_stuff(self):
                """Custom code"""

            def run(self):
                result = self.do_stuff()
                artifact_name = datanectar.get_artifact_name(self)
                self.storage_layer.store(artifact_name, result)

* Question/problem: I'm not sure if class decorators would allow us to inject in things like `self.storage_layer`. More research required to answer.

---




### How do we configure between various deployment target and task type permutations?

* How do we specify the artifact storage (S3 vs local vs Blob)?
* How do we define a task as a "Classifier" vs a "Regressor"?

#### Option 1: Classes for each permutation

Example code from current implementation:

	TheTaskClass = NectarLocalTask if os.getenv('ENV', 'local') == 'local' else NectarS3Task

This could lead to a proliferation of permutations to define various combinations like:

* NectarLocalClassifierTask
* NectarLocalRegressorTask
* NectarS3ClassifierTask
* NectarS3RegressorTask

#### Option 2: composition

In this option, we would pass in things like "artifact storage" and "model type".

Example code:

	import luigi
	import datanectar

	class FeatureExtractorTask(luigi.Task):
	    def __init__(self, task_type=None, model_type=None, storage_layer=None):
		self.task_type = task_type
		self.model_type = model_type
		self.storage_layer = storage_layer

	    def do_stuff(self):
		"""Custom code"""

	    def run(self):
		result = self.do_stuff()
		artifact_name = datanectar.get_artifact_name(self)
		self.storage_layer.store(artifact_name, result)

Notes:
* We could also use class decorators to inject __init__ arguments. However, this seems a little brittle if the parent class is not a datanectar task, because it would still require explicit calls to injected variables (like `self.storage_layer.store()`) in the methods. Or perhaps if we used method decorators also, that would be handled automatically?


## Conclusions

From Caleb:
* Given that we're planning to use Luigi, I think a basic class structure probably makes more sense than a pure functional architecture.
* Given the loss of power/flexibility building an abstract `datanectar.Task` class to interface between custom code and Luigi, I would prefer to use composition to call into datanectar functionality. Whether that is done by simply passing in things (like `storage_layer`) explicitly to constructor methods, or using decorators, I'm not sure.


