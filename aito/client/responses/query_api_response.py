"""Aito `Query API <https://aito.ai/docs/api/#query-api>`__ Response Class"""

from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Type, List, Dict, Any

from .aito_response import BaseResponse
from aito.utils._json_format import JsonFormat


class BaseHit(JsonFormat):
    """Aito `ResponseHit <https://aito.ai/docs/api/#schema-response-hit>`__"""
    def __init__(self, json: Dict):
        """

        :param json: the content of the hit
        :type json: Dict
        """
        self.json_schema_validate(json)
        self._json = json

    def __getitem__(self, item):
        if item not in self._json:
            raise KeyError(f'The hit does not contain field `{item}`. '
                           f'Please specify the field in the `select` clause of the query')
        return self._json[item]

    def __contains__(self, item):
        return item in self._json

    def __iter__(self):
        return self._json.keys()

    @classmethod
    def json_schema(cls):
        return {'type': 'object'}

    @property
    def json(self):
        """the content of the hit"""
        return self._json

    def to_json_serializable(self):
        return self._json

    @classmethod
    def from_deserialized_object(cls, obj: Any):
        return cls(obj)


class _BaseScoredHit(BaseHit, ABC):
    """Base class for `ResponseHit <https://aito.ai/docs/api/#schema-response-hit>`__ that contains a
    `score <https://aito.ai/docs/api/#schema-score-field>`__
    """
    @classmethod
    @abstractmethod
    def score_aliases(cls) -> list:
        """The aliases of the score. For example, a predict hit can have both $score and $p
        """
        pass

    @property
    def explanation(self) -> Dict:
        """returns the explanation of how the score was calculated

        :rtype: Dict
        """
        return self.__getitem__('$why')

    @property
    def score(self) -> float:
        """returns the score

        :rtype: float
        """
        score_field = next((alias for alias in self.score_aliases() if alias in self._json), None)
        if score_field is None:
            raise KeyError(f'The hit does not contain the score field. Please specify one of '
                           f'{"|".join(self.score_aliases())} in the `select` clause of the query')
        return self._json[score_field]


class ScoredHit(_BaseScoredHit):
    """`ResponseHit <https://aito.ai/docs/api/#schema-response-hit>`__ that contains a
    `score <https://aito.ai/docs/api/#schema-score-field>`__"""
    @classmethod
    def score_aliases(cls) -> list:
        return ['$score']


class ProbabilityHit(_BaseScoredHit):
    """`ResponseHit <https://aito.ai/docs/api/#schema-response-hit>`__ with probability"""
    @classmethod
    def score_aliases(cls) -> list:
        return ['$score', '$p']

    @property
    def predicted_probability(self) -> float:
        """return the predicted probability

        :rtype: float
        """
        return self.score


class RelateHit(BaseHit):
    """`ResponseHit <https://aito.ai/docs/api/#schema-response-hit>`__ of the
    `Relate query <https://aito.ai/docs/api/#post-api-v1-relate>`__

    Contains statistical information between pair of features
    """
    @property
    def frequencies(self):
        """return frequencies information of the relation"""
        return self.__getitem__('fs')

    @property
    def probabilities(self):
        """return probabilities information of the relation"""
        return self.__getitem__('ps')


HitType = TypeVar('HitType', bound=BaseHit)


class _GenericHitsResponse(BaseResponse, Generic[HitType], ABC):
    """HitsResponse with information of the HitType"""
    def __init__(self, json):
        """

        :param json: the original JSON response
        :type json: Dict
        """
        super().__init__(json)
        self._hits = [self.hit_cls(hit) for hit in json['hits']]

    @property
    @abstractmethod
    def hit_cls(self) -> Type[HitType]:
        """the class of the hit

        :rtype: Type[HitType]
        """
        pass

    @classmethod
    def json_schema(cls):
        return {
            'type': 'object',
            'properties': {
                'offset': {'type': 'integer'},
                'total': {'type': 'integer'},
                'hits': {'type': 'array', 'items': {'type': 'object'}}
            },
            'required': ['offset', 'total', 'hits']
        }

    @property
    def offset(self) -> int:
        """the number of hits that is skipped

        :rtype: int
        """
        return self._json['offset']

    @property
    def total(self) -> int:
        """the total number of hits

        :rtype: int
        """
        return self._json['total']

    @property
    def hits(self) -> List[HitType]:
        """the returned hits

        :rtype: List[HitType]
        """
        return self._hits

    @property
    def first_hit(self) -> HitType:
        """return the first hit

        :rtype: HitType
        """
        return self._hits[0]


class HitsResponse(_GenericHitsResponse[BaseHit]):
    """The response contains entries or `hits <https://aito.ai/docs/api/#schema-hits>`__ returned for a given query"""
    @property
    def hit_cls(self) -> Type[HitType]:
        return BaseHit


class SearchResponse(_GenericHitsResponse[BaseHit]):
    """Response of the `Search query <https://aito.ai/docs/api/#post-api-v1-search>`__"""
    @property
    def hit_cls(self) -> Type[HitType]:
        return BaseHit


class PredictResponse(_GenericHitsResponse[ProbabilityHit]):
    """Response of the `Predict query <https://aito.ai/docs/api/#post-api-v1-predict>`__"""
    @property
    def hit_cls(self) -> Type[HitType]:
        return ProbabilityHit

    @property
    def predictions(self) -> List[ProbabilityHit]:
        """return a list of predictions in descending order of the estimated probability

        :return:
        :rtype:
        """
        # noinspection PyTypeChecker
        return self.hits

    @property
    def top_prediction(self) -> ProbabilityHit:
        """return the prediction with the highest probability

        :rtype: Dict
        """
        # noinspection PyTypeChecker
        return self.first_hit


class RecommendResponse(_GenericHitsResponse[ProbabilityHit]):
    """Response of the `Recommend query <https://aito.ai/docs/api/#post-api-v1-recommend>`__"""
    @property
    def hit_cls(self) -> Type[HitType]:
        return ProbabilityHit

    @property
    def recommendations(self) -> List[ProbabilityHit]:
        """return a list of recommendations in descending order of the estimated probability of the specified goal

        :return:
        :rtype:
        """
        # noinspection PyTypeChecker
        return self.hits

    @property
    def top_recommendation(self) -> ProbabilityHit:
        """return the recommendation with the highest probability of the goal

        :rtype: Dict
        """
        # noinspection PyTypeChecker
        return self.first_hit


class SimilarityResponse(_GenericHitsResponse[ScoredHit]):
    """Response of the `Similarity query <https://aito.ai/docs/api/#post-api-v1-similarity>`__"""
    @property
    def hit_cls(self) -> Type[HitType]:
        return ScoredHit

    @property
    def similar_entries(self) -> List[ScoredHit]:
        """return a list of similar entries in descending order of the similarity score

        :return:
        :rtype:
        """
        # noinspection PyTypeChecker
        return self.hits

    @property
    def most_similar_entry(self) -> ScoredHit:
        """return the most similar entry

        :rtype: Dict
        """
        # noinspection PyTypeChecker
        return self.first_hit


class MatchResponse(_GenericHitsResponse[ProbabilityHit]):
    """Response of the `Match query <https://aito.ai/docs/api/#post-api-v1-match>`__"""
    @property
    def hit_cls(self) -> Type[HitType]:
        return ProbabilityHit

    @property
    def matches(self) -> List[ProbabilityHit]:
        """return a list of matches in descending order of the estimated probability

        :return:
        :rtype:
        """
        # noinspection PyTypeChecker
        return self.hits

    @property
    def top_match(self) -> ProbabilityHit:
        """return the match with the highest probability

        :rtype: Dict
        """
        # noinspection PyTypeChecker
        return self.first_hit


class RelateResponse(_GenericHitsResponse[RelateHit]):
    """Response of the `Relate query <https://aito.ai/docs/api/#post-api-v1-relate>`__"""
    @property
    def hit_cls(self) -> Type[HitType]:
        return RelateHit

    @property
    def relations(self) -> List[RelateHit]:
        # noinspection PyTypeChecker
        return self.hits


class EvaluateResponse(BaseResponse):
    """Response of the `Evaluate query <https://aito.ai/docs/api/#post-api-v1-evaluate>`__"""
    @property
    def accuracy(self) -> float:
        """test evaluation accuracy

        :rtype: float
        """
        return self.__getitem__('accuracy')

    @property
    def test_sample_count(self) -> int:
        """the number of entries in the table that was used as test set

        :rtype: int
        """
        return self.__getitem__('testSamples')

    @property
    def train_sample_count(self) -> int:
        """the number of entries in the table that was used to train Aito during evaluation

        :rtype: int
        """
        return self.__getitem__('trainSamples')
