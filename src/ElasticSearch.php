<?php
/**
 * @author Jan Wyszynski
 */

namespace Codeception\Module;

use Codeception\Module;
use Elasticsearch\Client;

class Elasticsearch extends Module
{
    /** @var  \Elasticsearch\Client */
    private $elasticSearch;

    public function __construct($config = null)
    {
        // terminology: see = isXyz => true/false, have = create, grab = get => data

        if (!isset($config['hosts'])) {
            throw new \Exception('please configure hosts for ElasticSearch codeception module');
        }

        if (isset($config['hosts']) && !is_array($config['hosts'])) {
            $config['hosts'] = array($config['hosts']);
        }
        $this->config = (array)$config;

        parent::__construct();
    }

    public function _initialize()
    {
        /*
         * elastic search config
         * hosts - array of ES hosts
         * dic - ES dictionary
         */

        $this->elasticSearch = new Client($this->config);
    }

    /**
     * check if an item exists in a given index
     *
     * @param string $index index name
     * @param string $type item type
     * @param string $id item id
     *
     * @return array
     */
    public function seeItemExistsInElasticsearch($index, $type, $id)
    {
        return $this->elasticSearch->exists(
            [
                'index' => $index,
                'type' => $type,
                'id' => $id
            ]
        );
    }


    /**
     * grab an item from search index
     *
     * @param null $index
     * @param null $type
     * @param string $queryString
     *
     * @return array
     */
    public function grabAnItemFromElasticsearch($index = null, $type = null, $queryString = '*')
    {
        $result = $this->elasticSearch->search(
            [
                'index' => $index,
                'type' => $type,
                'q' => $queryString,
                'size' => 1
            ]
        );

        return !empty($result['hits']['hits'])
            ? $result['hits']['hits'][0]['_source']
            : array();
    }


}