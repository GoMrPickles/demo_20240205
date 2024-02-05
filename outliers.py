"""
Simplest file to test data for outliers. May move to Jupyter notebook.
"""

# Example: Training an ECOD detector
from pyod.models.ecod import ECOD
clf = ECOD()
clf.fit(X_train)
y_train_scores = clf.decision_scores_  # Outlier scores for training data
y_test_scores = clf.decision_function(X_test)  # Outlier scores for test data